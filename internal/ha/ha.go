// Package ha implements the Patroni HA state machine.
// Mirrors patroni/ha.py (core paths: election, follow, promote, demote,
// bootstrap, crash recovery). Deliberately simplified: no async executor
// (actions run synchronously), no Citus/standby-cluster/failsafe modes,
// no replication slots management, no rewind (pg_rewind).
package ha

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/patroni/patroni-go/internal/config"
	"github.com/patroni/patroni-go/internal/dcs"
	"github.com/patroni/patroni-go/internal/postgres"
	"github.com/patroni/patroni-go/internal/watchdog"
)

const patroniGoVersion = "4.1.5-go"

// Ha runs one HA cycle per loop and holds all cluster interaction state.
type Ha struct {
	cfg      *config.Config
	dcs      dcs.DCS
	pg       *postgres.Postgresql
	watchdog watchdog.Watchdog

	cluster *dcs.Cluster

	NodeName string

	LastMessage string

	Recovering bool

	JoinAborted bool

	apiURL string
}

// New wires the HA state machine.
func New(cfg *config.Config, d dcs.DCS, pg *postgres.Postgresql, wd watchdog.Watchdog) *Ha {
	h := &Ha{
		cfg:      cfg,
		dcs:      d,
		pg:       pg,
		watchdog: wd,
		NodeName: cfg.Name(),
	}
	rest := cfg.RestAPISection()
	listen := "127.0.0.1:8008"
	if v, ok := rest["listen"].(string); ok && v != "" {
		listen = v
	}
	connect := listen
	if v, ok := rest["connect_address"].(string); ok && v != "" {
		connect = v
	}
	h.apiURL = "http://" + connect
	return h
}

// LoadClusterFromDCS re-reads cluster state into h.cluster.
func (h *Ha) LoadClusterFromDCS() error {
	cluster, err := h.dcs.GetCluster()
	if err != nil {
		h.cluster = nil
		return err
	}
	h.cluster = cluster
	return nil
}

// Cluster returns the last loaded cluster state.
func (h *Ha) Cluster() *dcs.Cluster { return h.cluster }

func (h *Ha) hasLock(checkLease bool) bool {
	if h.cluster == nil || h.cluster.Leader == nil {
		return false
	}
	return h.cluster.Leader.Name() == h.NodeName
}

// IsLeader reports whether this node currently holds the leader lock.
func (h *Ha) IsLeader() bool { return h.hasLock(true) }

// IsPaused reports whether pause mode is active.
func (h *Ha) IsPaused() bool {
	if h.cluster != nil && h.cluster.IsPaused() {
		return true
	}
	return false
}

// RunCycle executes one HA loop iteration and returns the status message.
// Mirrors Ha._run_cycle dispatch order.
func (h *Ha) RunCycle() string {
	defer func() {
		if !h.IsPaused() {
			h.TouchMember()
		}
	}()
	if err := h.LoadClusterFromDCS(); err != nil {
		log.Printf("[ha] error communicating with DCS: %v", err)
		return h.handleDCSError()
	}
	if h.IsPaused() {
		_ = h.watchdog.Disable()
		h.touchMemberState()
		return "PAUSE: continue to run as " + string(h.pg.Role)
	}
	if h.hasLock(false) && !dcs.SysidValid(h.cluster.Initialize) {
		sysid := h.pg.SysID
		if sysid == "" {
			if _, err := h.pg.ReadSysID(); err != nil {
				return "failed to read system ID: " + err.Error()
			}
			sysid = h.pg.SysID
		}
		if _, err := h.dcs.Initialize(false, sysid); err != nil {
			return "failed to write initialize key: " + err.Error()
		}
	}
	if h.hasLock(false) && (h.cluster.Config == nil || len(h.cluster.Config.Data) == 0) {
		if dyn := h.cfg.DynamicConfiguration(); len(dyn) > 0 {
			if b, err := json.Marshal(dyn); err == nil {
				_, _ = h.dcs.SetConfigValue(string(b), 0)
			}
			if err := h.LoadClusterFromDCS(); err != nil {
				return "failed to reload cluster: " + err.Error()
			}
		}
	}
	empty, err := h.pg.DataDirEmpty()
	if err != nil {
		return "data directory is not accessible: " + err.Error()
	}
	if empty {
		return h.handleEmptyDataDir()
	}
	if !dcs.SysidValid(h.pg.SysID) {
		if _, err := h.pg.ReadSysID(); err != nil {
			return "data dir is not empty, but system ID is invalid; consider doing reinitialize"
		}
	}
	if dcs.SysidValid(h.cluster.Initialize) && h.cluster.Initialize != h.pg.SysID {
		return fmt.Sprintf("FATAL: system ID mismatch, node %s belongs to a different cluster: %s != %s",
			h.NodeName, h.cluster.Initialize, h.pg.SysID)
	}
	if !h.pg.IsHealthy() {
		if h.pg.State == postgres.StateRunning || h.pg.State == postgres.StateStarting {
			h.pg.SetState(postgres.StateCrashed)
		}
		return h.recover()
	}
	if h.pg.Role == postgres.RoleUninitialized {
		h.refreshRole()
	}
	var msg string
	if h.cluster.IsUnlocked() {
		msg = h.processUnhealthyCluster()
	} else {
		msg = h.processHealthyCluster()
	}
	if h.pg.Role == postgres.RolePrimary {
		_ = h.watchdog.Keepalive()
	}
	h.LastMessage = msg
	return msg
}

func (h *Ha) handleDCSError() string {
	if !h.pg.IsRunning() {
		return "DCS is not accessible"
	}
	if h.pg.IsPrimary() {
		log.Printf("[ha] demoting self because DCS is not accessible and I was a leader")
		h.demote("")
		return "demoted self because DCS is not accessible and I was a leader"
	}
	return "DCS is not accessible"
}

func (h *Ha) handleEmptyDataDir() string {
	h.pg.SetRole(postgres.RoleUninitialized)
	if h.pg.IsRunning() {
		_ = h.pg.Stop("immediate", 30*time.Second)
	}
	_ = h.watchdog.Disable()
	if h.hasLock(false) {
		if err := h.dcs.DeleteLeader(); err != nil {
			log.Printf("[ha] failed to delete leader key: %v", err)
		}
		return "released leader key voluntarily as data dir is empty and currently leader"
	}
	if h.IsPaused() {
		return "running with empty data directory"
	}
	if !h.cluster.IsUnlocked() {
		return "data directory is empty, waiting for leader to establish cluster"
	}
	return h.bootstrap()
}

func (h *Ha) bootstrap() string {
	if !h.cluster.IsUnlocked() && dcs.SysidValid(h.cluster.Initialize) {
		return "cluster is already initialized"
	}
	log.Printf("[ha] bootstrapping new cluster")
	sysid := h.pg.SysID
	if sysid != "" {
		if applied, err := h.dcs.Initialize(true, sysid); err != nil || !applied {
			return "bootstrap interrupted: initialize key taken by another node"
		}
	}
	if err := h.pg.Bootstrap(h.cfg.BootstrapSection()); err != nil {
		_, _ = h.dcs.CancelInitialization()
		return "bootstrap failed: " + err.Error()
	}
	if h.pg.SysID == "" {
		_, _ = h.pg.ReadSysID()
	}
	if _, err := h.dcs.Initialize(true, h.pg.SysID); err != nil {
		return "failed to write initialize key: " + err.Error()
	}
	if dyn := h.cfg.DynamicConfiguration(); len(dyn) > 0 {
		if b, err := json.Marshal(dyn); err == nil {
			_, _ = h.dcs.SetConfigValue(string(b), 0)
		}
	}
	ok, err := h.dcs.AttemptToAcquireLeader()
	if err != nil || !ok {
		log.Printf("[ha] lost bootstrap leader race, stopping postgres: err=%v acquired=%v", err, ok)
		_ = h.pg.Stop("fast", 1*time.Minute)
		return "lost the bootstrap leader race, restarting as a secondary"
	}
	h.pg.SetRole(postgres.RolePrimary)
	h.pg.SetState(postgres.StateRunning)
	_ = h.watchdog.Keepalive()
	h.Recovering = false
	h.LastMessage = "bootstrapped self as a leader"
	return h.LastMessage
}

func (h *Ha) recover() string {
	if h.pg.IsRunning() {
		_ = h.pg.Stop("immediate", 30*time.Second)
	}
	if err := h.pg.Start(time.Duration(h.cfg.RetryTimeout()) * time.Second); err != nil {
		h.Recovering = true
		return "postgres is not running"
	}
	h.Recovering = true
	h.refreshRole()
	if h.pg.Role == postgres.RolePrimary {
		return "started as a primary despite not having the leader lock, will demote"
	}
	if h.hasLock(false) {
		return "starting as readonly because i had the leader lock"
	}
	return "starting as a secondary"
}

func (h *Ha) refreshRole() {
	if h.pg.IsPrimary() {
		h.pg.SetRole(postgres.RolePrimary)
		h.pg.SetState(postgres.StateRunning)
	} else if h.pg.IsRunning() {
		h.pg.SetRole(postgres.RoleReplica)
		h.pg.SetState(postgres.StateRunning)
	}
}

func (h *Ha) processUnhealthyCluster() string {
	if h.cluster.Failover != nil && h.cluster.Failover.Candidate != "" &&
		h.cluster.Failover.Candidate != h.NodeName {
		if leader := h.memberByName(h.cluster.Failover.Candidate); leader != nil {
			_ = h.follow(leader.ConnURL())
		}
		return "adding leader vote for " + h.cluster.Failover.Candidate
	}
	if !h.failoverPossible() {
		if h.pg.Role == postgres.RolePrimary || (h.pg.IsRunning() && h.pg.IsPrimary()) {
			_ = h.pg.Stop("fast", 1*time.Minute)
			h.pg.SetRole(postgres.RoleReplica)
		}
		return "delaying wal based election because my wal location is not the latest"
	}
	acquired, err := h.dcs.AttemptToAcquireLeader()
	if err != nil {
		return "failed to acquire leader lock: " + err.Error()
	}
	if !acquired {
		if err := h.LoadClusterFromDCS(); err == nil && !h.cluster.IsUnlocked() {
			if leader := h.cluster.Leader.Member; leader != nil && leader.Name != h.NodeName {
				_ = h.follow(leader.ConnURL())
			}
		}
		return "failed to acquire leader lock"
	}
	return h.promote()
}

func (h *Ha) failoverPossible() bool {
	if h.pg.Role == postgres.RoleUninitialized {
		return true
	}
	myLSN, err := h.pg.LastOperation()
	if err != nil {
		return false
	}
	for _, m := range h.cluster.Members {
		if m.Name == h.NodeName || m.Tag("nofailover") {
			continue
		}
		if lsn := m.XlogLocation(); lsn > 0 && lsn > myLSN {
			return false
		}
	}
	return true
}

func (h *Ha) promote() string {
	h.pg.SetRole(postgres.RolePrimary)
	h.pg.SetState(postgres.StateRunning)
	if h.pg.IsRunning() && !h.pg.IsPrimary() {
		if err := h.pg.Promote(int(h.cfg.RetryTimeout())); err != nil {
			log.Printf("[ha] promote failed: %v", err)
		}
	}
	_ = h.watchdog.Keepalive()
	if h.cluster.Failover != nil && h.cluster.Failover.Candidate == h.NodeName {
		_, _ = h.dcs.SetFailoverValue("", 0)
		h.clearFailoverKey()
	}
	msg := "promoted self to leader by acquiring the leader lock"
	h.LastMessage = msg
	return msg
}

func (h *Ha) clearFailoverKey() {
	cluster := h.cluster
	if cluster == nil || cluster.Failover == nil {
		return
	}
	_, _ = h.dcs.SetFailoverValue("", cluster.Failover.Version)
}

func (h *Ha) processHealthyCluster() string {
	if h.hasLock(false) {
		lastLSN, err := h.pg.LastOperation()
		if err != nil {
			lastLSN = 0
		}
		ok, err := h.dcs.UpdateLeader(h.cluster, lastLSN)
		if err != nil || !ok {
			log.Printf("[ha] failed to update leader lock: %v", err)
			h.demote("")
			return "demoted self because I do not have the leader lock"
		}
		if h.cluster.Failover != nil && h.cluster.Failover.Candidate != "" &&
			h.cluster.Failover.Candidate != h.NodeName {
			cand := h.memberByName(h.cluster.Failover.Candidate)
			if cand != nil && h.demote(cand.ConnURL()) {
				_, _ = h.dcs.SetFailoverValue("", h.cluster.Failover.Version)
				return "manual switchover to " + cand.Name + ", demoting self"
			}
		}
		h.pg.SetRole(postgres.RolePrimary)
		h.pg.SetState(postgres.StateRunning)
		return "no action. I am (p:" + strconvBool(h.pg.IsPrimary()) + "), " + h.NodeName + ", leader is " + h.cluster.Leader.Name()
	}
	leader := h.cluster.Leader.Member
	if leader == nil {
		leader = h.cluster.GetMember(h.cluster.Leader.Name(), true)
	}
	if h.pg.IsRunning() && h.pg.IsPrimary() {
		leaderURL := ""
		if leader != nil {
			leaderURL = leader.ConnURL()
		}
		h.demote(leaderURL)
		return "demoting self because I do not have the leader lock and I was a leader"
	}
	if leader != nil && leader.Name != h.NodeName {
		if err := h.follow(leader.ConnURL()); err != nil {
			log.Printf("[ha] follow failed: %v", err)
		}
	}
	if h.Recovering {
		h.Recovering = false
	}
	return "no action. I am (p:" + strconvBool(h.pg.IsPrimary()) + "), " + h.NodeName + ", leader is " + h.cluster.Leader.Name()
}

func (h *Ha) follow(leaderConnURL string) error {
	params := map[string]any{}
	return h.pg.Follow(leaderConnURL, params)
}

func (h *Ha) demote(followURL string) bool {
	_ = h.watchdog.Disable()
	wasPrimary := h.pg.IsRunning() && h.pg.IsPrimary()
	if wasPrimary {
		if err := h.pg.Stop("fast", time.Duration(h.cfg.RetryTimeout())*time.Second); err != nil {
			log.Printf("[ha] demote: failed to stop postgres: %v", err)
		}
	}
	h.pg.SetRole(postgres.RoleReplica)
	if followURL != "" {
		if err := h.follow(followURL); err != nil {
			log.Printf("[ha] demote: failed to follow %s: %v", followURL, err)
		}
	} else if err := h.dcs.DeleteLeader(); err != nil {
		log.Printf("[ha] demote: failed to delete leader key: %v", err)
	}
	return true
}

func (h *Ha) memberByName(name string) *dcs.Member {
	if h.cluster == nil {
		return nil
	}
	return h.cluster.GetMember(name, true)
}

// Watch blocks up to timeout waiting for the leader key to change.
func (h *Ha) Watch(timeout time.Duration) bool {
	if h.cluster == nil || h.cluster.IsUnlocked() || h.hasLock(false) {
		time.Sleep(timeout)
		return false
	}
	return h.dcs.Watch(h.cluster.Leader.Version, timeout)
}

// Shutdown performs graceful shutdown.
func (h *Ha) Shutdown() {
	if h.IsPaused() {
		log.Printf("[ha] leader key is not deleted and PostgreSQL is not stopped due paused state")
		_ = h.watchdog.Disable()
		return
	}
	wasLeader := h.hasLock(false)
	if err := h.pg.Stop("fast", time.Duration(h.cfg.RetryTimeout())*time.Second); err != nil {
		log.Printf("[ha] postgres shutdown failed, leader key not removed: %v", err)
		return
	}
	if wasLeader {
		if err := h.dcs.DeleteLeader(); err != nil {
			log.Printf("[ha] failed to delete leader key on shutdown: %v", err)
		}
	}
	data := map[string]any{"state": "stopped", "role": string(h.pg.Role), "api_url": h.apiURL}
	_, _ = h.dcs.TouchMember(data)
}

// TouchMember publishes member state to the DCS.
func (h *Ha) TouchMember() bool {
	data := h.memberData()
	ok, err := h.dcs.TouchMember(data)
	if err != nil {
		log.Printf("[ha] failed to touch member: %v", err)
	}
	return ok
}

func (h *Ha) touchMemberState() {
	data := h.memberData()
	data["state"] = string(h.pg.State)
	_, _ = h.dcs.TouchMember(data)
}

func (h *Ha) memberData() map[string]any {
	state := string(h.pg.State)
	if h.pg.IsRunning() {
		state = "running"
	} else if empty, _ := h.pg.DataDirEmpty(); empty {
		state = "initdb"
	}
	role := string(h.pg.Role)
	if h.IsLeader() && h.pg.Role != postgres.RolePrimary {
		role = string(postgres.RolePrimary)
	}
	data := map[string]any{
		"state":    state,
		"role":     role,
		"api_url":  h.apiURL,
		"conn_url": h.pg.ConnURL,
		"version":  patroniGoVersion,
	}
	if h.pg.SysID != "" {
		data["sysid"] = h.pg.SysID
	}
	if tl, err := h.pg.Timeline(); err == nil && tl > 0 {
		data["timeline"] = tl
	} else if h.cluster != nil && h.cluster.Leader != nil {
		data["timeline"] = h.cluster.Leader.Timeline()
	}
	if lsn, err := h.pg.LastOperation(); err == nil && lsn > 0 {
		data["xlog_location"] = lsn
	}
	data["tags"] = map[string]any{
		"nofailover": false,
		"nosync":     false,
	}
	if v := os.Getenv("PATRONI_TAG_NOFAILOVER"); v == "true" {
		data["tags"].(map[string]any)["nofailover"] = true
	}
	return data
}

// RequestFailover handles POST /failover and /switchover.
func (h *Ha) RequestFailover(leader, candidate string, scheduled *time.Time) error {
	if h.cluster == nil {
		return errors.New("cluster is not available")
	}
	if leader != "" && leader != h.cluster.LeaderName() {
		return fmt.Errorf("leader name %s doesn't match current leader %s", leader, h.cluster.LeaderName())
	}
	if candidate == "" {
		return errors.New("candidate is required")
	}
	if candidate == h.cluster.LeaderName() {
		return errors.New("candidate is already the leader")
	}
	if h.memberByName(candidate) == nil {
		return fmt.Errorf("candidate %s is not a member of the cluster", candidate)
	}
	failover := map[string]any{"candidate": candidate}
	if leader != "" {
		failover["leader"] = leader
	}
	if scheduled != nil {
		failover["scheduled_at"] = scheduled.UTC().Format(time.RFC3339Nano)
	}
	b, err := json.Marshal(failover)
	if err != nil {
		return err
	}
	version := int64(0)
	if h.cluster.Failover != nil {
		version = h.cluster.Failover.Version
	}
	applied, err := h.dcs.SetFailoverValue(string(b), version)
	if err != nil {
		return err
	}
	if !applied {
		return errors.New("failover key was updated concurrently, try again")
	}
	return nil
}

// RequestRestart handles POST /restart.
func (h *Ha) RequestRestart(body map[string]any) error {
	log.Printf("[ha] restart requested: %v", body)
	if err := h.pg.Restart(time.Duration(h.cfg.RetryTimeout()) * time.Second); err != nil {
		return err
	}
	h.refreshRole()
	return nil
}

// Reinitialize handles POST /reinitialize: wipe local datadir and clone
// from the leader via pg_basebackup.
func (h *Ha) Reinitialize(body map[string]any) error {
	if h.cluster == nil || h.cluster.IsUnlocked() || h.cluster.Leader.Name() == h.NodeName {
		return errors.New("reinitialize is only allowed on replicas of an initialized cluster")
	}
	leader := h.cluster.Leader.Member
	if leader == nil {
		return errors.New("leader member is unknown")
	}
	log.Printf("[ha] reinitialize from %s requested", leader.Name)
	if h.pg.IsRunning() {
		if err := h.pg.Stop("fast", 2*time.Minute); err != nil {
			return fmt.Errorf("failed to stop postgres: %w", err)
		}
	}
	if err := wipeDir(h.pg.DataDir); err != nil {
		return err
	}
	host, port, err := splitConnURL(leader.ConnURL())
	if err != nil {
		return err
	}
	if err := h.pgBasebackup(host, port); err != nil {
		return err
	}
	if err := h.pg.Start(2 * time.Minute); err != nil {
		return err
	}
	return h.follow(leader.ConnURL())
}

func (h *Ha) pgBasebackup(host, port string) error {
	bin, err := exec.LookPath("pg_basebackup")
	if err != nil && h.pg.BinDir != "" {
		bin = filepath.Join(h.pg.BinDir, "pg_basebackup")
	}
	if bin == "" {
		return errors.New("pg_basebackup not found")
	}
	args := []string{"-h", host, "-p", port, "-U", "replicator", "-D", h.pg.DataDir, "-X", "stream", "--checkpoint=fast"}
	cmd := exec.Command(bin, args...)
	cmd.Env = append(os.Environ(), "PGPASSWORD="+os.Getenv("PATRONI_REPLICATION_PASSWORD"))
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("pg_basebackup failed: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func splitConnURL(connURL string) (string, string, error) {
	u, err := url.Parse(connURL)
	if err != nil {
		return "", "", err
	}
	port := u.Port()
	if port == "" {
		port = "5432"
	}
	return u.Hostname(), port, nil
}

func wipeDir(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return os.MkdirAll(dir, 0o700)
		}
		return err
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

// Status builds the /patroni endpoint payload for this node.
func (h *Ha) Status() map[string]any {
	state := string(h.pg.State)
	if h.pg.IsRunning() {
		state = "running"
	} else {
		state = "stopped"
	}
	role := string(h.pg.Role)
	if h.IsLeader() {
		role = string(postgres.RolePrimary)
	} else if h.pg.IsRunning() && h.pg.IsPrimary() {
		role = string(postgres.RolePrimary)
	}
	ret := map[string]any{
		"state":                      state,
		"role":                       role,
		"patroni":                    map[string]any{"version": patroniGoVersion, "scope": h.cfg.Scope(), "name": h.NodeName},
		"pending_restart":            false,
		"database_system_identifier": h.pg.SysID,
	}
	if st := h.pg.PostmasterStartTime(); st != "" {
		ret["postmaster_start_time"] = st
	}
	if v := h.pg.ServerVersion(); v > 0 {
		ret["server_version"] = v
	}
	if tl, err := h.pg.Timeline(); err == nil && tl > 0 {
		ret["timeline"] = tl
	}
	if lsn, err := h.pg.LastOperation(); err == nil && lsn > 0 {
		ret["xlog_location"] = lsn
	}
	if h.cluster != nil {
		if h.cluster.Leader != nil {
			ret["leader"] = h.cluster.Leader.Name()
		}
		if h.cluster.Config != nil {
			ret["sync_standby"] = syncStandbyOf(h.cluster, h.NodeName)
		}
	}
	return ret
}

func syncStandbyOf(c *dcs.Cluster, name string) string {
	var names []string
	for _, m := range c.Members {
		if m.Role() == "sync_standby" && m.Name != name {
			names = append(names, m.Name)
		}
	}
	sort.Strings(names)
	return strings.Join(names, ",")
}

// Liveness returns the HTTP status code for GET /liveness.
func (h *Ha) Liveness() int { return 200 }

// Readiness returns HTTP status code and message for GET /readiness.
func (h *Ha) Readiness() (int, string) {
	if h.pg.IsHealthy() {
		return 200, ""
	}
	if !h.pg.IsRunning() {
		return 503, "postgres is not running"
	}
	return 503, "postgres is not accepting connections"
}

func strconvBool(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
