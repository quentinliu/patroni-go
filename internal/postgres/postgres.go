// Package postgres manages the local PostgreSQL instance: process control,
// role detection, configuration file management and bootstrap.
// Mirrors patroni/postgresql/__init__.py (core subset).
package postgres

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/patroni/patroni-go/internal/dcs"
)

// Role is the patroni-reported role of the local postgres instance.
type Role string

const (
	RolePrimary       Role = "primary"
	RoleReplica       Role = "replica"
	RoleSyncStandby   Role = "sync_standby"
	RoleQuorumStandby Role = "quorum_standby"
	RoleStandbyLeader Role = "standby_leader"
	RoleSecondary     Role = "secondary"
	RoleUninitialized Role = "uninitialized"
	RoleVirtual       Role = "virtual"
	RoleDemoted       Role = "demoted"
)

// State is the lifecycle state of the local postgres instance.
type State string

const (
	StateRunning  State = "running"
	StateStarting State = "starting"
	StateStopped  State = "stopped"
	StateCrashed  State = "crashed"
)

// Postgresql manages the local PostgreSQL server.
type Postgresql struct {
	Name  string
	State State
	Role  Role

	DataDir   string
	BinDir    string
	ConfigDir string
	ConnURL   string
	Port      int

	SysID string

	Scope              string
	Parameters         map[string]any
	RecoveryParameters map[string]any

	CbCalled bool

	section       map[string]any
	superuser     string
	replUser      string
	replPassword  string
	pgHBA         []string
	postmasterPID int
	startTime     time.Time
	serverVersion int64
	connString    string
}

// New validates the postgresql configuration section and constructs the
// controller without touching the filesystem.
func New(name string, section map[string]any) (*Postgresql, error) {
	if section == nil {
		return nil, errors.New("postgresql configuration section is required")
	}
	dataDir, _ := section["data_dir"].(string)
	if dataDir == "" {
		return nil, errors.New("postgresql.data_dir is required")
	}
	p := &Postgresql{
		Name:      name,
		State:     StateStopped,
		Role:      RoleUninitialized,
		DataDir:   dataDir,
		BinDir:    strSection(section, "bin_dir", ""),
		Port:      5432,
		section:   section,
		superuser: "postgres",
	}
	p.section = section
	if cd := strSection(section, "config_dir", ""); cd != "" {
		p.ConfigDir = cd
	} else {
		p.ConfigDir = dataDir
	}
	params, _ := section["parameters"].(map[string]any)
	p.Parameters = params
	if params != nil {
		if v, ok := params["port"].(any); ok {
			p.Port = anyInt(v, 5432)
		}
	}
	if connAddr := strSection(section, "connect_address", ""); connAddr != "" {
		p.ConnURL = "postgres://" + connAddr + "/postgres"
	} else {
		host := "127.0.0.1"
		if params != nil {
			if la, ok := params["listen_addresses"].(string); ok && la != "" && la != "*" {
				host = la
			}
		}
		p.ConnURL = fmt.Sprintf("postgres://%s:%d/postgres", host, p.Port)
	}
	if auth, ok := section["authentication"].(map[string]any); ok {
		if su, ok := auth["superuser"].(map[string]any); ok {
			p.superuser = strSection(su, "username", p.superuser)
		}
		if rp, ok := auth["replication"].(map[string]any); ok {
			p.replUser = strSection(rp, "username", "replicator")
			p.replPassword = strSection(rp, "password", "")
		}
	}
	if p.replUser == "" {
		p.replUser = "replicator"
	}
	if hba, ok := section["pg_hba"].([]any); ok {
		for _, line := range hba {
			if s, ok := line.(string); ok {
				p.pgHBA = append(p.pgHBA, s)
			}
		}
	}
	return p, nil
}

func strSection(m map[string]any, key, def string) string {
	if m == nil {
		return def
	}
	if v, ok := m[key].(string); ok && v != "" {
		return v
	}
	return def
}

func anyInt(v any, def int) int {
	switch x := v.(type) {
	case int:
		return x
	case int64:
		return int(x)
	case float64:
		return int(x)
	case string:
		if n, err := strconv.Atoi(x); err == nil {
			return n
		}
	}
	return def
}

func (p *Postgresql) binary(name string) (string, error) {
	if p.BinDir != "" {
		full := filepath.Join(p.BinDir, name)
		if _, err := os.Stat(full); err == nil {
			return full, nil
		}
	}
	return exec.LookPath(name)
}

func (p *Postgresql) run(timeout time.Duration, name string, args ...string) (string, error) {
	bin, err := p.binary(name)
	if err != nil {
		return "", fmt.Errorf("%s not found: %w", name, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = append(os.Environ(), "LC_ALL=C", "PGDATA="+p.DataDir)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("%s %s failed: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

// DataDirEmpty reports whether the data directory is missing or empty.
func (p *Postgresql) DataDirEmpty() (bool, error) {
	entries, err := os.ReadDir(p.DataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return true, nil
		}
		return false, err
	}
	return len(entries) == 0, nil
}

// ReadSysID runs pg_controldata and stores the database system identifier.
func (p *Postgresql) ReadSysID() (string, error) {
	out, err := p.run(p.ctlTimeout(), "pg_controldata", "-D", p.DataDir)
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "Database system identifier:") {
			sysid := strings.TrimSpace(strings.TrimPrefix(line, "Database system identifier:"))
			if sysid != "" {
				p.SysID = sysid
			}
			return sysid, nil
		}
	}
	return "", errors.New("database system identifier not found in pg_controldata output")
}

func (p *Postgresql) ctlTimeout() time.Duration { return 30 * time.Second }

// IsRunning reports whether a postmaster process is alive for this data dir.
func (p *Postgresql) IsRunning() bool {
	pid, err := p.readPostmasterPID()
	if err != nil {
		p.postmasterPID = 0
		return false
	}
	if err := syscall.Kill(pid, 0); err != nil {
		p.postmasterPID = 0
		return false
	}
	p.postmasterPID = pid
	return true
}

func (p *Postgresql) readPostmasterPID() (int, error) {
	data, err := os.ReadFile(filepath.Join(p.DataDir, "postmaster.pid"))
	if err != nil {
		return 0, err
	}
	lines := strings.SplitN(string(data), "\n", 6)
	if len(lines) < 1 {
		return 0, errors.New("postmaster.pid is empty")
	}
	pid, err := strconv.Atoi(strings.TrimSpace(lines[0]))
	if err != nil {
		return 0, fmt.Errorf("invalid pid in postmaster.pid: %w", err)
	}
	return pid, nil
}

// IsHealthy is true when postgres is running and accepting connections.
func (p *Postgresql) IsHealthy() bool {
	if !p.IsRunning() {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := p.connect(ctx)
	if err != nil {
		return false
	}
	conn.Close(context.Background())
	return true
}

// IsStarting is true while postgres is up but still doing recovery.
func (p *Postgresql) IsStarting() bool { return p.State == StateStarting }

// IsPrimary reports whether the running instance is a primary.
func (p *Postgresql) IsPrimary() bool {
	if !p.IsRunning() {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := p.connect(ctx)
	if err != nil {
		return false
	}
	defer conn.Close(context.Background())
	var inRecovery bool
	if err := conn.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&inRecovery); err != nil {
		return false
	}
	return !inRecovery
}

func (p *Postgresql) connect(ctx context.Context) (*pgx.Conn, error) {
	if p.connString == "" {
		p.connString = p.buildConnString()
	}
	return pgx.Connect(ctx, p.connString)
}

func (p *Postgresql) buildConnString() string {
	socketDir := p.DataDir
	host := socketDir
	return fmt.Sprintf("postgres://%s@%s:%d/postgres?host=%s&port=%d",
		p.superuser, "127.0.0.1", p.Port, url.QueryEscape(host), p.Port)
}

// Start launches postgres via pg_ctl and waits until it is ready.
func (p *Postgresql) Start(timeout time.Duration) error {
	if err := p.WriteConfigFiles(); err != nil {
		return err
	}
	secs := int(timeout.Seconds())
	if secs < 30 {
		secs = 30
	}
	_, err := p.run(timeout, "pg_ctl", "-D", p.DataDir, "-w", "-t", strconv.Itoa(secs), "-l",
		filepath.Join(p.ConfigDir, "postgresql.log"), "start")
	if err != nil {
		p.SetState(StateCrashed)
		return err
	}
	p.startTime = time.Now()
	if inRecovery := p.checkInRecovery(); inRecovery {
		p.SetState(StateStarting)
	} else {
		p.SetState(StateRunning)
		p.SetRole(RolePrimary)
	}
	return nil
}

func (p *Postgresql) checkInRecovery() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := p.connect(ctx)
	if err != nil {
		return false
	}
	defer conn.Close(context.Background())
	var inRecovery bool
	_ = conn.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&inRecovery)
	return inRecovery
}

// Stop stops postgres with the given mode ("fast", "immediate", "smart").
func (p *Postgresql) Stop(mode string, timeout time.Duration) error {
	if mode == "" {
		mode = "fast"
	}
	secs := int(timeout.Seconds())
	if secs < 30 {
		secs = 30
	}
	_, err := p.run(timeout, "pg_ctl", "-D", p.DataDir, "-w", "-t", strconv.Itoa(secs), "-m", mode, "stop")
	if err != nil && !p.IsRunning() {
		// process already gone: treat as success
		err = nil
	}
	p.postmasterPID = 0
	p.SetState(StateStopped)
	return err
}

// Restart stops then starts postgres.
func (p *Postgresql) Restart(timeout time.Duration) error {
	if err := p.Stop("fast", timeout); err != nil {
		return err
	}
	return p.Start(timeout)
}

// Reload sends SIGHUP to the postmaster.
func (p *Postgresql) Reload() error {
	_, err := p.run(p.ctlTimeout(), "pg_ctl", "-D", p.DataDir, "reload")
	return err
}

// Promote promotes a standby to primary.
func (p *Postgresql) Promote(waitSeconds int) error {
	_ = os.Remove(filepath.Join(p.ConfigDir, "conf.d", "patroni-recovery.conf"))
	_, err := p.run(time.Duration(waitSeconds+10)*time.Second, "pg_ctl",
		"-D", p.DataDir, "-w", "-t", strconv.Itoa(waitSeconds), "promote")
	if err != nil {
		return err
	}
	deadline := time.Now().Add(time.Duration(waitSeconds) * time.Second)
	for time.Now().Before(deadline) {
		if p.IsRunning() && p.IsPrimary() {
			p.SetState(StateRunning)
			p.SetRole(RolePrimary)
			return nil
		}
		time.Sleep(time.Second)
	}
	return errors.New("promote timed out waiting for pg_is_in_recovery to become false")
}

// WriteConfigFiles renders postgresql.conf, pg_hba.conf and conf.d/patroni.conf.
func (p *Postgresql) WriteConfigFiles() error {
	if err := os.MkdirAll(filepath.Join(p.ConfigDir, "conf.d"), 0o700); err != nil {
		return err
	}
	conf := fmt.Sprintf(`# generated by patroni-go, do not edit
listen_addresses = '%s'
port = %d
unix_socket_directories = '%s'
hba_file = '%s'
ident_file = '%s'
include_dir = 'conf.d'
cluster_name = '%s'
`,
		listenAddresses(p.Parameters), p.Port, p.DataDir,
		filepath.Join(p.ConfigDir, "pg_hba.conf"),
		filepath.Join(p.ConfigDir, "pg_ident.conf"),
		p.Scope)
	if err := os.WriteFile(filepath.Join(p.ConfigDir, "postgresql.conf"), []byte(conf), 0o600); err != nil {
		return err
	}
	hba := p.pgHBA
	if len(hba) == 0 {
		hba = []string{
			"local   all             all                                     trust",
			"host    all             all             127.0.0.1/32            trust",
			"host    all             all             ::1/128                 trust",
			"host    replication     replicator      0.0.0.0/0               trust",
			"host    replication     replicator      ::/0                    trust",
		}
	}
	if err := os.WriteFile(filepath.Join(p.ConfigDir, "pg_hba.conf"), []byte(strings.Join(hba, "\n")+"\n"), 0o600); err != nil {
		return err
	}
	return p.writeDynamicConf()
}

func listenAddresses(params map[string]any) string {
	if la, ok := params["listen_addresses"].(string); ok && la != "" {
		return la
	}
	return "*"
}

func (p *Postgresql) writeDynamicConf() error {
	var b strings.Builder
	for k, v := range p.Parameters {
		fmt.Fprintf(&b, "%s = '%v'\n", k, v)
	}
	for k, v := range p.RecoveryParameters {
		fmt.Fprintf(&b, "%s = '%v'\n", k, v)
	}
	return os.WriteFile(filepath.Join(p.ConfigDir, "conf.d", "patroni.conf"), []byte(b.String()), 0o600)
}

// RecoveryParametersSet applies recovery parameters and signals postgres to
// re-read them. Returns true when a restart would be required.
func (p *Postgresql) RecoveryParametersSet(params map[string]any) bool {
	changed := !mapsEqualStr(p.RecoveryParameters, params)
	p.RecoveryParameters = params
	if !changed {
		return false
	}
	if err := p.writeDynamicConf(); err != nil {
		log.Printf("[postgres] failed to write recovery parameters: %v", err)
		return false
	}
	if p.IsRunning() {
		if err := p.Reload(); err != nil {
			log.Printf("[postgres] reload failed: %v", err)
		}
	}
	return false
}

func mapsEqualStr(a, b map[string]any) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		bv, ok := b[k]
		if !ok || fmt.Sprintf("%v", v) != fmt.Sprintf("%v", bv) {
			return false
		}
	}
	return true
}

// LastOperation returns the current WAL LSN of the local instance.
func (p *Postgresql) LastOperation() (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := p.connect(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close(context.Background())
	var inRecovery bool
	if err := conn.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&inRecovery); err != nil {
		return 0, err
	}
	query := "SELECT pg_current_wal_lsn()::text"
	if inRecovery {
		query = "SELECT GREATEST(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn())::text"
	}
	var lsn string
	if err := conn.QueryRow(ctx, query).Scan(&lsn); err != nil {
		return 0, err
	}
	return dcs.ParseLSNString(lsn), nil
}

// PostmasterStartTime returns the postmaster start time (ISO string).
func (p *Postgresql) PostmasterStartTime() string {
	if p.postmasterPID == 0 && !p.IsRunning() {
		return ""
	}
	if !p.startTime.IsZero() {
		return p.startTime.UTC().Format(time.RFC3339Nano)
	}
	return ""
}

// ServerVersion returns the numeric server version (e.g. 150000) or 0.
func (p *Postgresql) ServerVersion() int64 {
	if p.serverVersion != 0 {
		return p.serverVersion
	}
	data, err := os.ReadFile(filepath.Join(p.DataDir, "PG_VERSION"))
	if err != nil {
		return 0
	}
	major := strings.TrimSpace(string(data))
	n, err := strconv.Atoi(major)
	if err != nil {
		return 0
	}
	if n < 10 {
		n = n * 10000
	} else {
		n = n * 10000
	}
	p.serverVersion = int64(n)
	return p.serverVersion
}

// Timeline returns the current timeline.
func (p *Postgresql) Timeline() (int64, error) {
	if p.IsRunning() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		conn, err := p.connect(ctx)
		if err == nil {
			defer conn.Close(context.Background())
			var tl int64
			if err := conn.QueryRow(ctx, "SELECT timeline_id FROM pg_control_checkpoint()").Scan(&tl); err == nil {
				return tl, nil
			}
		}
	}
	out, err := p.run(p.ctlTimeout(), "pg_controldata", "-D", p.DataDir)
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "Latest checkpoint's TimeLineID:") {
			n, err := strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(line, "Latest checkpoint's TimeLineID:")), 10, 64)
			if err != nil {
				return 0, err
			}
			return n, nil
		}
	}
	return 0, errors.New("TimeLineID not found in pg_controldata output")
}

// Bootstrap initializes a brand new cluster data directory.
func (p *Postgresql) Bootstrap(bootstrapSection map[string]any) error {
	empty, err := p.DataDirEmpty()
	if err != nil {
		return err
	}
	if !empty {
		return fmt.Errorf("data directory %s is not empty", p.DataDir)
	}
	if err := os.MkdirAll(p.DataDir, 0o700); err != nil {
		return err
	}
	initdbArgs := []string{"-D", p.DataDir, "--auth-local=trust", "--auth-host=trust", "-U", p.superuser}
	if bs, ok := bootstrapSection["initdb"].([]any); ok {
		for _, arg := range bs {
			if s, ok := arg.(string); ok {
				initdbArgs = append(initdbArgs, s)
			}
		}
	}
	if out, err := p.run(10*time.Minute, "initdb", initdbArgs...); err != nil {
		return fmt.Errorf("initdb failed: %w", err)
	} else if out != "" {
		log.Printf("[postgres] initdb completed")
	}
	if _, err := p.ReadSysID(); err != nil {
		return err
	}
	if err := p.Start(2 * time.Minute); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := p.connect(ctx)
	if err != nil {
		return fmt.Errorf("connect after bootstrap: %w", err)
	}
	defer conn.Close(context.Background())
	createRepl := fmt.Sprintf("CREATE USER %s WITH REPLICATION", p.replUser)
	if p.replPassword != "" {
		createRepl += fmt.Sprintf(" PASSWORD '%s'", strings.ReplaceAll(p.replPassword, "'", "''"))
	}
	if _, err := conn.Exec(ctx, createRepl); err != nil {
		log.Printf("[postgres] create replication user: %v", err)
	}
	p.SetState(StateRunning)
	p.SetRole(RolePrimary)
	p.CbCalled = true
	return nil
}

// Follow applies standby settings to follow the given primary.
func (p *Postgresql) Follow(primaryConnURL string, recoveryParams map[string]any) error {
	params := map[string]any{}
	for k, v := range recoveryParams {
		params[k] = v
	}
	if primaryConnURL != "" {
		ci := buildPrimaryConninfo(primaryConnURL, p.replUser, p.replPassword)
		if ci != "" {
			params["primary_conninfo"] = ci
		}
	}
	needRestart := !p.IsRunning() || p.IsPrimary()
	p.RecoveryParametersSet(params)
	if needRestart {
		standby := filepath.Join(p.DataDir, "standby.signal")
		if p.IsRunning() {
			if err := p.Stop("fast", 1*time.Minute); err != nil {
				return err
			}
		}
		if err := os.WriteFile(standby, nil, 0o600); err != nil {
			return err
		}
		return p.Start(2 * time.Minute)
	}
	return nil
}

func buildPrimaryConninfo(connURL, user, password string) string {
	u, err := url.Parse(connURL)
	if err != nil {
		return ""
	}
	host := u.Hostname()
	port := u.Port()
	if port == "" {
		port = "5432"
	}
	if user == "" {
		user = "replicator"
	}
	ci := fmt.Sprintf("host=%s port=%s user=%s application_name=%%s", host, port, user)
	if password != "" {
		ci += " password=" + password
	}
	return ci
}

// ResetState re-reads state from disk after startup.
func (p *Postgresql) ResetState() {
	if p.IsRunning() {
		p.SetState(StateRunning)
	} else {
		p.SetState(StateStopped)
	}
}

// SetRole updates the tracked role.
func (p *Postgresql) SetRole(r Role) { p.Role = r }

// SetState updates the tracked state.
func (p *Postgresql) SetState(s State) { p.State = s }
