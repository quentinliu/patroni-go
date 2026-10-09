package etcd3

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/patroni/patroni-go/internal/dcs"
)

const (
	keyInitialize = "initialize"
	keyConfig     = "config"
	keyMembers    = "members/"
	keyLeader     = "leader"
	keyFailover   = "failover"
	keyHistory    = "history"
	keyOptime     = "optime/leader"
)

type Etcd3 struct {
	Scope        string
	Namespace    string
	Name         string
	TTL          int64
	RetryTimeout time.Duration
	loopWait     int64

	client *clientv3.Client

	mu        sync.Mutex
	leaseID   clientv3.LeaseID
	leaseExp  time.Time
	hasLeader bool
}

func New(section map[string]any) (*Etcd3, error) {
	e := &Etcd3{
		Scope:        strVal(section, "scope", "batman"),
		Namespace:    strVal(section, "namespace", "/service/"),
		Name:         strVal(section, "name", ""),
		TTL:          int64(intVal(section, "ttl", 30)),
		RetryTimeout: time.Duration(intVal(section, "retry_timeout", 10)) * time.Second,
		loopWait:     int64(intVal(section, "loop_wait", 10)),
	}
	if e.TTL < 20 {
		e.TTL = 20
	}
	hosts := parseHosts(section)
	cfg := clientv3.Config{
		Endpoints:   hosts,
		DialTimeout: 5 * time.Second,
		Username:    strVal(section, "username", ""),
		Password:    strVal(section, "password", ""),
	}
	if ca := strVal(section, "cacert", ""); ca != "" || strVal(section, "cert", "") != "" {
		tlsCfg, err := loadTLS(section)
		if err != nil {
			return nil, err
		}
		cfg.TLS = tlsCfg
	}
	cli, err := clientv3.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("unable to create etcd3 client: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), e.RetryTimeout)
	defer cancel()
	if _, err := cli.Cluster.MemberList(ctx); err != nil {
		cli.Close()
		return nil, fmt.Errorf("unable to connect to etcd3 at %s: %w", strings.Join(hosts, ","), err)
	}
	e.client = cli
	log.Printf("[etcd3] connected to %s, scope=%s name=%s ttl=%d", strings.Join(hosts, ","), e.Scope, e.Name, e.TTL)
	return e, nil
}

var _ dcs.DCS = (*Etcd3)(nil)

func (e *Etcd3) base() string { return dcs.KeyPath(e.Namespace, e.Scope) }
func (e *Etcd3) path(suffix string) string {
	return e.base() + "/" + suffix
}

func (e *Etcd3) GetCluster() (*dcs.Cluster, error) {
	ctx, cancel := context.WithTimeout(context.Background(), e.RetryTimeout)
	defer cancel()
	resp, err := e.client.Get(ctx, e.base()+"/", clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("%w: %v", dcs.ErrDCSUnreachable, err)
	}
	cluster := &dcs.Cluster{}
	for _, kv := range resp.Kvs {
		key := strings.TrimPrefix(string(kv.Key), e.base()+"/")
		val := string(kv.Value)
		rev := kv.ModRevision
		switch {
		case key == keyInitialize:
			cluster.Initialize = val
		case key == keyConfig:
			cc := &dcs.ClusterConfig{Version: rev, Data: map[string]any{}}
			if err := json.Unmarshal([]byte(val), &cc.Data); err != nil {
				log.Printf("[etcd3] failed to parse config value: %v", err)
			}
			cluster.Config = cc
		case key == keyLeader:
			name := strings.TrimSpace(val)
			member := &dcs.Member{Version: rev, Name: name, Data: map[string]any{}}
			cluster.Leader = &dcs.Leader{Version: rev, Session: leaseOf(kv), Member: member}
		case key == keyFailover:
			f := &dcs.Failover{Version: rev}
			var data map[string]any
			if err := json.Unmarshal([]byte(val), &data); err == nil {
				f.FromData(data)
			}
			cluster.Failover = f
		case key == keyHistory:
			var lines []dcs.HistoryLine
			if err := json.Unmarshal([]byte(val), &lines); err == nil {
				cluster.History = &dcs.TimelineHistory{Version: rev, Lines: lines}
			}
		case key == keyOptime:
			cluster.LastLSN = dcs.ParseLSNString(strings.TrimSpace(val))
		case strings.HasPrefix(key, keyMembers):
			name := strings.TrimPrefix(key, keyMembers)
			if name == "" || strings.Contains(name, "/") {
				continue
			}
			m := &dcs.Member{Version: rev, Name: name, Session: leaseOf(kv), Data: map[string]any{}}
			if err := json.Unmarshal([]byte(val), &m.Data); err != nil {
				log.Printf("[etcd3] failed to parse member %s: %v", name, err)
			}
			cluster.Members = append(cluster.Members, m)
		}
	}
	if cluster.Leader != nil {
		if m := cluster.GetMember(cluster.Leader.Name(), false); m != nil {
			cluster.Leader.Member = m
			cluster.Leader.Session = m.Session
		}
	}
	return cluster, nil
}

func leaseOf(kv *mvccpb.KeyValue) int64 {
	if kv.Lease != 0 {
		return int64(kv.Lease)
	}
	return -1
}

func (e *Etcd3) ensureLease(isLeader bool) (clientv3.LeaseID, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), e.RetryTimeout)
	defer cancel()
	if e.leaseID != 0 && time.Now().Before(e.leaseExp) {
		if _, err := e.client.KeepAliveOnce(ctx, e.leaseID); err == nil {
			e.leaseExp = time.Now().Add(time.Duration(e.TTL) * time.Second)
			return e.leaseID, nil
		}
		e.leaseID = 0
	}
	lease, err := e.client.Grant(ctx, e.TTL)
	if err != nil {
		return 0, fmt.Errorf("%w: grant lease: %v", dcs.ErrDCSUnreachable, err)
	}
	e.leaseID = lease.ID
	e.leaseExp = time.Now().Add(time.Duration(e.TTL) * time.Second)
	return e.leaseID, nil
}

func (e *Etcd3) revokeLease() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.leaseID == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), e.RetryTimeout)
	defer cancel()
	_, _ = e.client.Revoke(ctx, e.leaseID)
	e.leaseID = 0
	e.leaseExp = time.Time{}
}

func (e *Etcd3) TouchMember(data map[string]any) (bool, error) {
	value, err := json.Marshal(data)
	if err != nil {
		return false, err
	}
	lease, err := e.ensureLease(false)
	if err != nil {
		return false, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), e.RetryTimeout)
	defer cancel()
	_, err = e.client.Put(ctx, e.path(keyMembers+e.Name), string(value), clientv3.WithLease(lease))
	if err != nil {
		return false, fmt.Errorf("%w: %v", dcs.ErrDCSUnreachable, err)
	}
	return true, nil
}

func (e *Etcd3) AttemptToAcquireLeader() (bool, error) {
	lease, err := e.ensureLease(true)
	if err != nil {
		return false, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), e.RetryTimeout)
	defer cancel()
	txn := e.client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(e.path(keyLeader)), "=", 0)).
		Then(clientv3.OpPut(e.path(keyLeader), e.Name, clientv3.WithLease(lease)))
	resp, err := txn.Commit()
	if err != nil {
		return false, fmt.Errorf("%w: %v", dcs.ErrDCSUnreachable, err)
	}
	return resp.Succeeded, nil
}

func (e *Etcd3) UpdateLeader(cluster *dcs.Cluster, lastLSN int64) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), e.RetryTimeout)
	defer cancel()
	resp, err := e.client.Get(ctx, e.path(keyLeader))
	if err != nil {
		return false, fmt.Errorf("%w: %v", dcs.ErrDCSUnreachable, err)
	}
	e.mu.Lock()
	mine := e.leaseID
	e.mu.Unlock()
	if len(resp.Kvs) == 0 || strings.TrimSpace(string(resp.Kvs[0].Value)) != e.Name {
		return false, dcs.ErrLeaderKeyLost
	}
	if mine == 0 || (resp.Kvs[0].Lease != 0 && int64(resp.Kvs[0].Lease) != int64(mine)) {
		return false, dcs.ErrLeaderKeyLost
	}
	if _, err := e.client.KeepAliveOnce(ctx, mine); err != nil {
		return false, fmt.Errorf("%w: keepalive: %v", dcs.ErrDCSUnreachable, err)
	}
	e.mu.Lock()
	e.leaseExp = time.Now().Add(time.Duration(e.TTL) * time.Second)
	e.mu.Unlock()
	if lastLSN > 0 {
		_ = e.WriteLeaderOptime(lastLSN)
	}
	return true, nil
}

func (e *Etcd3) DeleteLeader() error {
	ctx, cancel := context.WithTimeout(context.Background(), e.RetryTimeout)
	defer cancel()
	resp, err := e.client.Get(ctx, e.path(keyLeader))
	if err == nil && len(resp.Kvs) > 0 && strings.TrimSpace(string(resp.Kvs[0].Value)) == e.Name {
		_, err = e.client.Delete(ctx, e.path(keyLeader))
	}
	e.revokeLease()
	return err
}

func (e *Etcd3) WriteLeaderOptime(lastLSN int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), e.RetryTimeout)
	defer cancel()
	_, err := e.client.Put(ctx, e.path(keyOptime), strconv.FormatInt(lastLSN, 10))
	return err
}

func (e *Etcd3) SetFailoverValue(value string, version int64) (bool, error) {
	return e.casPut(e.path(keyFailover), value, version)
}

func (e *Etcd3) SetConfigValue(value string, version int64) (bool, error) {
	return e.casPut(e.path(keyConfig), value, version)
}

func (e *Etcd3) casPut(key, value string, version int64) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), e.RetryTimeout)
	defer cancel()
	txn := e.client.Txn(ctx)
	if version > 0 {
		txn = txn.If(clientv3.Compare(clientv3.ModRevision(key), "=", version))
	} else {
		txn = txn.If(clientv3.Compare(clientv3.ModRevision(key), "=", 0))
	}
	txn = txn.Then(clientv3.OpPut(key, value))
	resp, err := txn.Commit()
	if err != nil {
		return false, fmt.Errorf("%w: %v", dcs.ErrDCSUnreachable, err)
	}
	return resp.Succeeded, nil
}

func (e *Etcd3) SetHistoryValue(value string) (bool, error) {
	return e.casPut(e.path(keyHistory), value, 0)
}

func (e *Etcd3) Initialize(createNew bool, sysid string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), e.RetryTimeout)
	defer cancel()
	txn := e.client.Txn(ctx)
	if createNew {
		txn = txn.If(clientv3.Compare(clientv3.CreateRevision(e.path(keyInitialize)), "=", 0))
	}
	txn = txn.Then(clientv3.OpPut(e.path(keyInitialize), sysid))
	resp, err := txn.Commit()
	if err != nil {
		return false, fmt.Errorf("%w: %v", dcs.ErrDCSUnreachable, err)
	}
	return resp.Succeeded, nil
}

func (e *Etcd3) CancelInitialization() (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), e.RetryTimeout)
	defer cancel()
	resp, err := e.client.Delete(ctx, e.path(keyInitialize))
	if err != nil {
		return false, fmt.Errorf("%w: %v", dcs.ErrDCSUnreachable, err)
	}
	return resp.Deleted > 0, nil
}

func (e *Etcd3) DeleteCluster() (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), e.RetryTimeout)
	defer cancel()
	resp, err := e.client.Delete(ctx, e.base()+"/", clientv3.WithPrefix())
	if err != nil {
		return false, fmt.Errorf("%w: %v", dcs.ErrDCSUnreachable, err)
	}
	return resp.Deleted > 0, nil
}

func (e *Etcd3) Watch(leaderVersion int64, timeout time.Duration) bool {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	wch := e.client.Watch(ctx, e.path(keyLeader), clientv3.WithRev(leaderVersion+1))
	select {
	case _, ok := <-wch:
		return ok
	case <-ctx.Done():
		return false
	}
}

func (e *Etcd3) LoopWait() int64 { return e.loopWait }

func (e *Etcd3) ReloadConfig(cfg map[string]any) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if v := intVal(cfg, "ttl", 0); v >= 20 {
		e.TTL = int64(v)
	}
	if v := intVal(cfg, "retry_timeout", 0); v > 0 {
		e.RetryTimeout = time.Duration(v) * time.Second
	}
	if v := intVal(cfg, "loop_wait", 0); v > 0 {
		e.loopWait = int64(v)
	}
}

func strVal(m map[string]any, key, def string) string {
	if m == nil {
		return def
	}
	if v, ok := m[key].(string); ok && v != "" {
		return v
	}
	return def
}

func intVal(m map[string]any, key string, def int) int {
	if m == nil {
		return def
	}
	switch v := m[key].(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	case string:
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func parseHosts(section map[string]any) []string {
	var hosts []string
	switch v := section["hosts"].(type) {
	case []any:
		for _, h := range v {
			if s, ok := h.(string); ok {
				hosts = append(hosts, normalizeHost(s))
			}
		}
	case []string:
		for _, s := range v {
			hosts = append(hosts, normalizeHost(s))
		}
	}
	if len(hosts) == 0 {
		if h := strVal(section, "host", ""); h != "" {
			hosts = append(hosts, normalizeHost(h))
		}
	}
	if len(hosts) == 0 {
		hosts = []string{"127.0.0.1:2379"}
	}
	return hosts
}

func normalizeHost(h string) string {
	if strings.Contains(h, "://") {
		if i := strings.Index(h, "://"); i >= 0 {
			h = h[i+3:]
		}
	}
	if !strings.Contains(h, ":") {
		h += ":2379"
	}
	return h
}

func loadTLS(section map[string]any) (*tls.Config, error) {
	ca := strVal(section, "cacert", "")
	cert := strVal(section, "cert", "")
	key := strVal(section, "key", "")
	if ca == "" && cert == "" && key == "" {
		return nil, nil
	}
	if (cert == "") != (key == "") {
		return nil, errors.New("etcd3: cert and key must be provided together")
	}
	cfg := &tls.Config{}
	if ca != "" {
		pem, err := os.ReadFile(ca)
		if err != nil {
			return nil, fmt.Errorf("cacert %s: %w", ca, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("cacert %s: no valid certificates", ca)
		}
		cfg.RootCAs = pool
	}
	if cert != "" {
		pair, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return nil, fmt.Errorf("load client cert: %w", err)
		}
		cfg.Certificates = []tls.Certificate{pair}
	}
	return cfg, nil
}
