// Package watchdog provides Linux watchdog device support to fence a
// Patroni leader that lost access to the DCS. Mirrors patroni/watchdog.
package watchdog

import (
	"log"
	"sync"
)

// Watchdog is the safe-mode fencing interface. Mirrors Python Watchdog.
type Watchdog interface {
	IsRunning() bool
	IsHealthy() bool
	ReloadConfig(section map[string]any)
	Disable() error
	Keepalive() error
}

// Noop is a disabled watchdog used when safe_mode is not configured.
type Noop struct{}

func (Noop) IsRunning() bool             { return false }
func (Noop) IsHealthy() bool             { return true }
func (Noop) ReloadConfig(map[string]any) {}
func (Noop) Disable() error              { return nil }
func (Noop) Keepalive() error            { return nil }

// device implements the Watchdog interface on top of /dev/watchdog.
// On non-Linux platforms or when the device is absent it degrades to Noop
// behavior while still reporting IsRunning according to configuration.
type device struct {
	mu       sync.Mutex
	path     string
	slotName string
	open     bool
}

// New builds a watchdog from the `watchdog` config section. When the section
// is empty the returned watchdog is always a Noop.
func New(section map[string]any) Watchdog {
	if len(section) == 0 {
		return Noop{}
	}
	if v, ok := section["mode"].(string); ok && v == "off" {
		return Noop{}
	}
	d := &device{path: "/dev/watchdog"}
	if v, ok := section["device"].(string); ok && v != "" {
		d.path = v
	}
	if v, ok := section["slot_name"].(string); ok {
		d.slotName = v
	}
	return d
}

func (d *device) IsRunning() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.open
}

func (d *device) IsHealthy() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.open {
		return true
	}
	f, err := openWatchdog(d.path)
	if err != nil {
		return false
	}
	_ = f.Close()
	return true
}

func (d *device) ReloadConfig(section map[string]any) {
	// dynamic config can not change watchdog device safely while running;
	// only mode=off takes effect.
	if v, ok := section["mode"].(string); ok && v == "off" {
		_ = d.Disable()
	}
}

func (d *device) Disable() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.open {
		return nil
	}
	if err := closeWatchdog(d.path); err != nil {
		log.Printf("[watchdog] failed to close %s: %v", d.path, err)
		return err
	}
	d.open = false
	return nil
}

func (d *device) Keepalive() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.open {
		return nil
	}
	return keepaliveWatchdog(d.path)
}
