package dcs

import (
	"errors"
	"fmt"
	"path"
	"strings"
	"time"
)

// DCSError is returned when communication with the DCS fails.
// Mirrors Python patroni.exceptions.DCSError.
type DCSError struct {
	Msg string
}

func (e *DCSError) Error() string { return e.Msg }

// NewDCSError wraps err into DCSError.
func NewDCSError(msg string) *DCSError { return &DCSError{Msg: msg} }

// ErrDCSUnreachable is a sentinel error for DCS communication failures;
// wrap it with %w when returning from implementations.
var ErrDCSUnreachable = errors.New("communication with DCS failed")

// ErrLeaderKeyLost must be returned by UpdateLeader when the leader key
// could not be refreshed because this node no longer owns it.
var ErrLeaderKeyLost = errors.New("leader key lost")

// DCS abstracts the distributed configuration store.
// Mirrors Python patroni.dcs.AbstractDCS (subset used by the core HA loop).
type DCS interface {
	// GetCluster fetches the whole cluster state from the DCS.
	GetCluster() (*Cluster, error)

	// TouchMember writes member data under the member key, bound to a
	// session/lease with TTL. Returns true on success.
	TouchMember(data map[string]any) (bool, error)

	// AttemptToAcquireLeader tries to create the leader key bound to this
	// node session. Returns true when the lock is acquired.
	AttemptToAcquireLeader() (bool, error)

	// UpdateLeader refreshes the leader session/lease. Returns
	// ErrLeaderKeyLost when the lock no longer belongs to this node.
	UpdateLeader(cluster *Cluster, lastLSN int64) (bool, error)

	// DeleteLeader removes the leader key (voluntary demote / shutdown).
	DeleteLeader() error

	// WriteLeaderOptime stores the last known leader LSN.
	WriteLeaderOptime(lastLSN int64) error

	// SetFailoverValue writes the /failover key with CAS on version
	// (version <= 0 means unconditional).
	SetFailoverValue(value string, version int64) (bool, error)

	// SetConfigValue writes the /config key with CAS on version.
	SetConfigValue(value string, version int64) (bool, error)

	// SetHistoryValue writes the /history key.
	SetHistoryValue(value string) (bool, error)

	// Initialize writes the /initialize key with the cluster sysid.
	// When createNew is true the write must fail if the key exists.
	Initialize(createNew bool, sysid string) (bool, error)

	// CancelInitialization removes the /initialize key.
	CancelInitialization() (bool, error)

	// DeleteCluster removes all cluster keys (used on reinitialize).
	DeleteCluster() (bool, error)

	// Watch blocks up to timeout waiting for a change of the leader key.
	// leaderVersion <= 0 means watch on absence. Returns true when woken
	// before the timeout expires. Mirrors AbstractDCS.watch.
	Watch(leaderVersion int64, timeout time.Duration) bool

	// LoopWait returns the effective loop_wait value (seconds).
	LoopWait() int64

	// ReloadConfig re-applies dcs-related configuration (ttl, retry_timeout...).
	ReloadConfig(cfg map[string]any)
}

// CASResult is returned by conditional writes. true means the write applied.
type CASResult bool

func (r CASResult) String() string {
	if r {
		return "applied"
	}
	return "not applied"
}

// KeyPath builds a namespaced DCS key path, e.g. "/service/batman/leader".
// Mirrors Python AbstractDCS._base_path: {namespace}/{scope} with slashes
// collapsed, where namespace defaults to "/service/".
func KeyPath(namespace, scope string, suffix ...string) string {
	parts := append([]string{"/", strings.Trim(namespace, "/"), scope}, suffix...)
	return path.Join(parts...)
}

// StateEntry is a helper for building member data payload.
type StateEntry struct {
	Key   string
	Value any
}

// BuildMemberData assembles the canonical member payload written to DCS.
func BuildMemberData(connURL, apiURL, state, role string, timeline int64, extra map[string]any) map[string]any {
	data := map[string]any{
		"conn_url": connURL,
		"api_url":  apiURL,
		"state":    state,
		"role":     role,
	}
	if timeline > 0 {
		data["timeline"] = timeline
	}
	for k, v := range extra {
		if v != nil && fmt.Sprintf("%v", v) != "" {
			data[k] = v
		}
	}
	return data
}
