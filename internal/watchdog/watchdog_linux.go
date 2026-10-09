//go:build linux

package watchdog

import (
	"os"
	"syscall"
	"unsafe"
)

func openWatchdog(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_WRONLY, 0)
}

func keepaliveWatchdog(path string) error {
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write([]byte("1"))
	return err
}

func closeWatchdog(path string) error {
	f, err := os.OpenFile(path, os.O_WRONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	const ioctlWriteInt = 0x40045712 // _IOW('W', 2, int): WDIOC_SETOPTIONS would be complex; use magic close
	magic := []byte("V")
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, f.Fd(), ioctlWriteInt, uintptr(unsafe.Pointer(&magic[0])))
	if errno != 0 {
		return errno
	}
	return nil
}
