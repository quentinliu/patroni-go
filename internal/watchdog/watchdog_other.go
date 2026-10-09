//go:build !linux

package watchdog

import (
	"errors"
	"os"
)

func openWatchdog(string) (*os.File, error) {
	return nil, errors.New("watchdog device is only supported on linux")
}

func keepaliveWatchdog(string) error { return errors.New("watchdog device is only supported on linux") }

func closeWatchdog(string) error { return errors.New("watchdog device is only supported on linux") }
