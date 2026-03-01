//go:build debugtrace

package btree

import (
	"log"
	"os"
)

const debugTrace = true

var debugTraceLog *log.Logger

func init() {
	v := os.Getenv("BTREE_TRACE")
	if v == "" || v == "1" || v == "stderr" {
		debugTraceLog = log.New(os.Stderr, "[BTREE-TRACE] ", log.Lmicroseconds)
	} else {
		f, err := os.OpenFile(v, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			log.Printf("BTREE_TRACE: cannot open %s: %v, falling back to stderr", v, err)
			debugTraceLog = log.New(os.Stderr, "[BTREE-TRACE] ", log.Lmicroseconds)
		} else {
			debugTraceLog = log.New(f, "[BTREE-TRACE] ", log.Lmicroseconds)
		}
	}
}

func trace(format string, args ...any) {
	debugTraceLog.Printf(format, args...)
}
