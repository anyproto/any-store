package driver

import (
	"fmt"
	"runtime"
	"strings"
	"time"
	"unsafe"
)

// StalledConnections returns a list of stalled connections
// works only if stalledConnDetectorEnabled is true
func (c *ConnManager) StalledConnections(threshold time.Duration) (stalledTraces []string) {
	if !c.stalledConnDetectorEnabled {
		return
	}
	c.stalledConnStackMutex.Lock()
	defer c.stalledConnStackMutex.Unlock()
	for _, stack := range c.stalledConnStackTraces {
		if time.Since(time.Unix(int64(stack[0]), 0)) > threshold {
			duration, stackTrace := unpackStackWithFrames(stack)
			if duration > threshold {
				stalledTraces = append(stalledTraces, stackTrace)
			}
		}
	}
	return
}

func (c *ConnManager) stalledAcquireConn(conn *Conn) {
	if !c.stalledConnDetectorEnabled {
		return
	}
	stack := packStack(3)
	c.stalledConnStackMutex.Lock()
	defer c.stalledConnStackMutex.Unlock()
	c.stalledConnStackTraces[uintptr(unsafe.Pointer(conn))] = stack
}

func (c *ConnManager) stalledReleaseConn(conn *Conn) {
	if !c.stalledConnDetectorEnabled {
		return
	}
	c.stalledConnStackMutex.Lock()
	defer c.stalledConnStackMutex.Unlock()
	delete(c.stalledConnStackTraces, uintptr(unsafe.Pointer(conn)))
}

// packStack returns a slice of uintptrs representing the current stack, 0-element used to store the current timestamp
func packStack(skip int) []uintptr {
	// Allocate space for up to 32(-1) stack frames; adjust as needed.
	pcs := make([]uintptr, 32)
	// use first element to store current timestamp
	pcs[0] = uintptr(time.Now().Unix()) // on 32 bit systems it will not work after 2106, but who cares about 32 bit systems after 2106
	n := runtime.Callers(skip, pcs[1:31])
	return pcs[:n]
}

func unpackStackWithFrames(stack []uintptr) (time.Duration, string) {
	var (
		s        strings.Builder
		duration time.Duration
	)
	if stack[0] > 0 {
		// first element is timestamp
		duration = time.Since(time.Unix(int64(stack[0]), 0))
	}
	frames := runtime.CallersFrames(stack[1:])
	for {
		frame, more := frames.Next()
		s.WriteString(fmt.Sprintf("%s\n\t%s:%d\n", frame.Function, frame.File, frame.Line))
		if !more {
			break
		}
	}
	return duration, s.String()
}
