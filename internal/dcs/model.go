// Package dcs implements the distributed configuration store abstraction,
// mirroring patroni/dcs/__init__.py from the Python codebase.
package dcs

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Member represents a single member of the PostgreSQL cluster.
// Mirrors Python Member(version, name, session, data) NamedTuple.
type Member struct {
	// Version is the modification version (mod revision) of the member key in the DCS.
	Version int64
	// Name is the name of the cluster member.
	Name string
	// Session is the lease/session id (or TTL in seconds for non-leased stores). -1 when absent.
	Session int64
	// Data holds arbitrary member data: conn_url, api_url, state, role, tags, timeline, xlog_location, ...
	Data map[string]any
}

// Str returns a string value from member data.
func (m *Member) Str(key string) string {
	if m == nil || m.Data == nil {
		return ""
	}
	if v, ok := m.Data[key].(string); ok {
		return v
	}
	return ""
}

// ConnURL returns the PostgreSQL connection URL of this member.
func (m *Member) ConnURL() string { return m.Str("conn_url") }

// APIURL returns the REST API URL of the patroni instance on this member.
func (m *Member) APIURL() string { return m.Str("api_url") }

// State returns the member state: "running", "stopped", "starting", "crashed", "unknown"...
func (m *Member) State() string {
	if s := m.Str("state"); s != "" {
		return s
	}
	return "unknown"
}

// IsRunning reports whether the member is in "running" state.
func (m *Member) IsRunning() bool { return m.State() == "running" }

// Role returns the member role: "primary", "replica", "sync_standby", etc.
func (m *Member) Role() string { return m.Str("role") }

// RealRole returns the actual PostgreSQL role: "primary", "secondary", "standby_leader", etc.
func (m *Member) RealRole() string { return m.Str("real_role") }

// Timeline returns the member timeline as int64 (0 when unknown).
func (m *Member) Timeline() int64 {
	if m == nil {
		return 0
	}
	switch v := m.Data["timeline"].(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	case int:
		return int64(v)
	case string:
		n, _ := strconv.ParseInt(v, 10, 64)
		return n
	}
	return 0
}

// XlogLocation returns the current LSN (receive/flush/replay) of the member.
func (m *Member) XlogLocation() int64 {
	return ParseLSN(m.Data["xlog_location"])
}

// Tags returns the tags map from member data.
func (m *Member) Tags() map[string]any {
	if m == nil || m.Data == nil {
		return nil
	}
	if t, ok := m.Data["tags"].(map[string]any); ok {
		return t
	}
	return nil
}

// Tag returns a bool tag value by name.
func (m *Member) Tag(name string) bool {
	t := m.Tags()
	if t == nil {
		return false
	}
	v, _ := t[name].(bool)
	return v
}

// PatroniVersion returns the patroni version string recorded in member data.
func (m *Member) PatroniVersion() string { return m.Str("version") }

// LSN parses a value that may be an LSN given either as number or as "XX/XX" string.
func ParseLSN(v any) int64 {
	switch x := v.(type) {
	case float64:
		return int64(x)
	case int64:
		return x
	case int:
		return int64(x)
	case string:
		return ParseLSNString(x)
	}
	return 0
}

// ParseLSNString parses PostgreSQL LSN representation "X/X" into int64.
func ParseLSNString(s string) int64 {
	if s == "" {
		return 0
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n
	}
	parts := strings.SplitN(s, "/", 2)
	if len(parts) != 2 {
		return 0
	}
	hi, err1 := strconv.ParseInt(parts[0], 16, 64)
	lo, err2 := strconv.ParseInt(parts[1], 16, 64)
	if err1 != nil || err2 != nil {
		return 0
	}
	return hi<<32 | lo
}

// FormatLSN formats int64 LSN into PostgreSQL "X/X" representation.
func FormatLSN(lsn int64) string {
	return fmt.Sprintf("%X/%X", uint32(lsn>>32), uint32(lsn))
}

// Leader represents the leader key. Mirrors Python Leader NamedTuple.
type Leader struct {
	Version int64
	Session int64 // lease id; -1 when absent
	Member  *Member
}

// Name returns the leader member name.
func (l *Leader) Name() string {
	if l == nil || l.Member == nil {
		return ""
	}
	return l.Member.Name
}

// Timeline returns the timeline of the leader member.
func (l *Leader) Timeline() int64 {
	if l == nil {
		return 0
	}
	return l.Member.Timeline()
}

// Failover represents the contents of the /failover key.
type Failover struct {
	Version      int64
	Candidate    string
	Leader       string // for scheduled switchover: the current leader name
	ScheduledAt  *time.Time
	SanityPassed bool
}

// FromData parses failover payload (JSON map) into fields.
func (f *Failover) FromData(data map[string]any) {
	if data == nil {
		return
	}
	f.Candidate, _ = data["candidate"].(string)
	f.Leader, _ = data["leader"].(string)
	if ts, ok := data["scheduled_at"].(string); ok && ts != "" {
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05.999999-07:00", "2006-01-02T15:04:05"} {
			if t, err := time.Parse(layout, ts); err == nil {
				f.ScheduledAt = &t
				break
			}
		}
	}
}

// ClusterConfig represents the /config key holding dynamic configuration.
type ClusterConfig struct {
	Version int64
	Data    map[string]any
}

// IsPaused reports whether the cluster is in maintenance (pause) mode.
func (c *ClusterConfig) IsPaused() bool {
	if c == nil || c.Data == nil {
		return false
	}
	v, _ := c.Data["pause"].(bool)
	return v
}

// HistoryLine is [timeline, lsn, reason].
type HistoryLine []any

// TimelineHistory represents the /history key.
type TimelineHistory struct {
	Version int64
	Lines   []HistoryLine
}

// Cluster is the whole state of the patroni cluster as stored in DCS.
type Cluster struct {
	// Initialize holds the value of /initialize key: the sysid of the bootstrapped cluster.
	// Empty string means the key does not exist. Use SysidValid to interpret.
	Initialize string
	Config     *ClusterConfig
	Leader     *Leader
	// LastLSN is the leader optime (from /optime/leader) when available; 0 if unknown.
	LastLSN  int64
	History  *TimelineHistory
	Members  []*Member
	Failover *Failover
}

// IsUnlocked reports whether there is no leader key.
func (c *Cluster) IsUnlocked() bool { return c == nil || c.Leader == nil }

// HasMember reports whether the named member is part of the cluster.
func (c *Cluster) HasMember(name string) bool {
	if c == nil {
		return false
	}
	for _, m := range c.Members {
		if m.Name == name {
			return true
		}
	}
	return false
}

// GetMember returns the member by name. If fallbackToLeader is true and the
// name matches the leader, the leader member is returned as well.
func (c *Cluster) GetMember(name string, fallbackToLeader bool) *Member {
	if c == nil {
		return nil
	}
	for _, m := range c.Members {
		if m.Name == name {
			return m
		}
	}
	if fallbackToLeader && c.Leader != nil && c.Leader.Name() == name {
		return c.Leader.Member
	}
	return nil
}

// IsPaused reports whether pause mode is enabled in cluster config.
func (c *Cluster) IsPaused() bool { return c.Config.IsPaused() }

// LeaderName returns the current leader name ("" when unlocked).
func (c *Cluster) LeaderName() string {
	if c == nil || c.Leader == nil {
		return ""
	}
	return c.Leader.Name()
}

// Status converts the cluster into a JSON-friendly map (used by /cluster endpoint).
func (c *Cluster) Status() map[string]any {
	if c == nil {
		return nil
	}
	members := make([]map[string]any, 0, len(c.Members))
	for _, m := range c.Members {
		mm := map[string]any{
			"name":    m.Name,
			"role":    m.Role(),
			"state":   m.State(),
			"api_url": m.APIURL(),
		}
		if u := m.ConnURL(); u != "" {
			mm["conn_url"] = u
		}
		if tl := m.Timeline(); tl > 0 {
			mm["timeline"] = tl
		}
		if tags := m.Tags(); len(tags) > 0 {
			mm["tags"] = tags
		}
		if lsn := m.XlogLocation(); lsn > 0 {
			mm["xlog_location"] = lsn
		}
		members = append(members, mm)
	}
	ret := map[string]any{
		"members": members,
	}
	if c.Leader != nil && c.Leader.Member != nil {
		ret["leader"] = c.Leader.Name()
	}
	if c.Initialize != "" {
		ret["initialize"] = c.Initialize
	}
	if c.Config != nil && len(c.Config.Data) > 0 {
		ret["config"] = c.Config.Data
	}
	return ret
}

// SysidValid mirrors Python sysid_valid: a value is valid when it is not
// empty and not "false".
func SysidValid(v string) bool { return v != "" && v != "false" }

// SlotNameFromMemberName translates a member name to a valid replication slot name.
// Mirrors Python slot_name_from_member_name: lowercase, [-.] -> "_", other
// invalid chars encoded as uXXXX, truncated to 63 chars.
func SlotNameFromMemberName(name string) string {
	var b strings.Builder
	for _, c := range strings.ToLower(name) {
		switch {
		case c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_':
			b.WriteRune(c)
		case c == '-' || c == '.':
			b.WriteByte('_')
		default:
			fmt.Fprintf(&b, "u%04x", c)
		}
	}
	s := b.String()
	if len(s) > 63 {
		s = s[:63]
	}
	return s
}
