// Package config loads and manages Patroni configuration: local YAML file,
// PATRONI_* environment overrides and dynamic configuration stored in the DCS.
// Mirrors patroni/config.py (core subset).
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"
)

const (
	defaultNamespace    = "/service/"
	defaultScope        = "batman"
	defaultLoopWait     = 10
	defaultTTL          = 30
	defaultRetryTimeout = 10
)

// Config is the merged view of local configuration.
type Config struct {
	mu sync.RWMutex

	// Path is the path of the local configuration file.
	Path string

	// configuration holds the local YAML configuration.
	configuration map[string]any

	// dynamicConfiguration holds the configuration loaded from DCS /config key.
	dynamicConfiguration map[string]any

	// environment holds PATRONI_... environment overrides.
	environment map[string]any

	// cachePath is where dynamic configuration is cached
	// (<postgresql.data_dir>/patroni.dynamic.json equivalent, YAML here).
	cachePath string
}

// NewFromFile loads configuration from the YAML file at path and applies
// PATRONI_* environment overrides.
func NewFromFile(path string) (*Config, error) {
	c := &Config{Path: path}
	if err := c.loadLocal(); err != nil {
		return nil, err
	}
	c.loadEnvironment()
	return c, nil
}

func (c *Config) loadLocal() error {
	data, err := os.ReadFile(c.Path)
	if err != nil {
		return fmt.Errorf("unable to read configuration file %s: %w", c.Path, err)
	}
	cfg := map[string]any{}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return fmt.Errorf("unable to parse configuration file %s: %w", c.Path, err)
	}
	if cfg == nil {
		cfg = map[string]any{}
	}
	c.configuration = cfg
	return nil
}

// loadEnvironment applies PATRONI_<NAME> environment variables.
// Mirrors Python Config._load_environment_configuration: dotted keys map to
// nested maps, values are converted to int/float/bool when they look like one.
func (c *Config) loadEnvironment() {
	env := map[string]any{}
	for _, kv := range os.Environ() {
		i := strings.IndexByte(kv, '=')
		if i <= 0 {
			continue
		}
		k, v := kv[:i], kv[i+1:]
		if !strings.HasPrefix(k, "PATRONI_") {
			continue
		}
		name := strings.ToLower(strings.TrimPrefix(k, "PATRONI_"))
		parts := strings.Split(name, "_")
		if len(parts) > 1 && parts[0] == "ctl" {
			continue
		}
		cur := env
		for j, p := range parts {
			if j == len(parts)-1 {
				cur[p] = convertEnvValue(v)
			} else {
				next, ok := cur[p].(map[string]any)
				if !ok {
					next = map[string]any{}
					cur[p] = next
				}
				cur = next
			}
		}
	}
	c.environment = env
}

func convertEnvValue(v string) any {
	switch strings.ToLower(v) {
	case "true":
		return true
	case "false":
		return false
	case "null", "none", "":
		return nil
	}
	if n, err := strconv.Atoi(v); err == nil {
		return n
	}
	if f, err := strconv.ParseFloat(v, 64); err == nil {
		return f
	}
	return v
}

// Get returns a top-level configuration value, environment overrides applied.
func (c *Config) Get(key string) any {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if v, ok := c.environment[key]; ok {
		return v
	}
	return c.configuration[key]
}

// Section returns a nested configuration map merged with its env override.
func (c *Config) Section(name string) map[string]any {
	c.mu.RLock()
	base, _ := c.configuration[name].(map[string]any)
	c.mu.RUnlock()
	merged := map[string]any{}
	for k, v := range base {
		merged[k] = v
	}
	c.mu.RLock()
	if env, ok := c.environment[name].(map[string]any); ok {
		for k, v := range env {
			merged[k] = v
		}
	}
	c.mu.RUnlock()
	return merged
}

// SetLocal updates a local configuration value in memory (used on reload).
func (c *Config) SetLocal(key string, value any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.configuration[key] = value
}

// Name returns the node name.
func (c *Config) Name() string {
	if v, ok := c.Get("name").(string); ok {
		return v
	}
	return ""
}

// Scope returns the cluster scope name.
func (c *Config) Scope() string {
	if v, ok := c.Get("scope").(string); ok && v != "" {
		return v
	}
	return defaultScope
}

// Namespace returns the DCS key namespace (with leading slash).
func (c *Config) Namespace() string {
	if v, ok := c.Get("namespace").(string); ok && v != "" {
		if !strings.HasPrefix(v, "/") {
			v = "/" + v
		}
		if !strings.HasSuffix(v, "/") {
			v += "/"
		}
		return v
	}
	return defaultNamespace
}

// LoopWait returns loop_wait in seconds.
func (c *Config) LoopWait() int64 { return c.intDefault("loop_wait", defaultLoopWait, 1) }

// TTL returns ttl in seconds.
func (c *Config) TTL() int64 { return c.intDefault("ttl", defaultTTL, 20) }

// RetryTimeout returns retry_timeout in seconds.
func (c *Config) RetryTimeout() int64 {
	return c.intDefault("retry_timeout", defaultRetryTimeout, 1)
}

func (c *Config) intDefault(key string, def, min int64) int64 {
	switch v := c.Get(key).(type) {
	case int:
		r := int64(v)
		if r < min {
			return min
		}
		return r
	case float64:
		return int64(v)
	case string:
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			if n < min {
				return min
			}
			return n
		}
	}
	return def
}

// RestAPISection returns the restapi configuration section.
func (c *Config) RestAPISection() map[string]any { return c.Section("restapi") }

// PostgresqlSection returns the postgresql configuration section.
func (c *Config) PostgresqlSection() map[string]any { return c.Section("postgresql") }

// BootstrapSection returns the bootstrap configuration section.
func (c *Config) BootstrapSection() map[string]any { return c.Section("bootstrap") }

// DynamicConfiguration returns the dynamic configuration map (from DCS).
func (c *Config) DynamicConfiguration() map[string]any {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.dynamicConfiguration
}

// SetDynamicConfiguration stores dynamic configuration and reports whether
// it changed compared to the previous value.
func (c *Config) SetDynamicConfiguration(cfg map[string]any) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	changed := !mapsEqual(c.dynamicConfiguration, cfg)
	c.dynamicConfiguration = cfg
	return changed
}

// Configuration returns the dynamic configuration as local "postgresql.parameters"
// overrides merged view for the HA layer.
func (c *Config) MergedPostgresqlParameters() map[string]any {
	params := map[string]any{}
	if ps := c.PostgresqlSection(); ps != nil {
		if p, ok := ps["parameters"].(map[string]any); ok {
			for k, v := range p {
				params[k] = v
			}
		}
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if dc := c.dynamicConfiguration; dc != nil {
		if p, ok := dc["postgresql"].(map[string]any); ok {
			if pp, ok := p["parameters"].(map[string]any); ok {
				for k, v := range pp {
					params[k] = v
				}
			}
		}
	}
	return params
}

// SaveCache persists dynamic configuration next to the local config file.
func (c *Config) SaveCache() error {
	c.mu.RLock()
	dc := c.dynamicConfiguration
	c.mu.RUnlock()
	if dc == nil {
		return nil
	}
	if c.cachePath == "" {
		dir, _ := filepath.Split(c.Path)
		c.cachePath = filepath.Join(dir, "patroni.dynamic.yaml")
	}
	data, err := yaml.Marshal(dc)
	if err != nil {
		return err
	}
	return os.WriteFile(c.cachePath, data, 0o600)
}

// LoadCache restores cached dynamic configuration (used before DCS access).
func (c *Config) LoadCache() {
	if c.cachePath == "" {
		dir, _ := filepath.Split(c.Path)
		c.cachePath = filepath.Join(dir, "patroni.dynamic.yaml")
	}
	data, err := os.ReadFile(c.cachePath)
	if err != nil {
		return
	}
	cfg := map[string]any{}
	if err := yaml.Unmarshal(data, &cfg); err == nil {
		c.SetDynamicConfiguration(cfg)
	}
}

// ReloadLocal re-reads the local configuration file.
func (c *Config) ReloadLocal() error { return c.loadLocal() }

// DCSSection returns the DCS configuration section, with global keys
// (namespace, scope, ttl, loop_wait, retry_timeout, name) propagated in.
// Mirrors Python get_dcs config propagation.
func (c *Config) DCSSection(dcsName string) map[string]any {
	section := c.Section(dcsName)
	for _, p := range []string{"namespace", "name", "scope", "loop_wait", "ttl", "retry_timeout"} {
		if v := c.Get(p); v != nil {
			if _, exists := section[p]; !exists {
				section[p] = v
			}
		}
	}
	return section
}

// DetectDCS returns the configured DCS backend name by checking top-level
// sections: etcd3, etcd, consul, zookeeper, kubernetes, raft.
func (c *Config) DetectDCS() string {
	for _, name := range []string{"etcd3", "etcd", "consul", "zookeeper", "kubernetes", "exhibitor", "raft"} {
		if s := c.Section(name); len(s) > 0 {
			return name
		}
	}
	return ""
}

func mapsEqual(a, b map[string]any) bool {
	if a == nil && b == nil {
		return true
	}
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		bv, ok := b[k]
		if !ok || !deepEqual(v, bv) {
			return false
		}
	}
	return true
}

func deepEqual(a, b any) bool {
	am, aok := a.(map[string]any)
	bm, bok := b.(map[string]any)
	if aok && bok {
		return mapsEqual(am, bm)
	}
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}
