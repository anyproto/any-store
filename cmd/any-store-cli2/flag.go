package main

import (
	"strings"

	"github.com/spf13/pflag"
)

var (
	fExec    = pflag.StringP("exec", "e", "", "execute command and exit")
	fVersion = pflag.BoolP("version", "v", false, "print version and exit")
	fHelp    = pflag.BoolP("help", "h", false, "print help")
)

// longFlagNames lists the multi-char flag names that users may reasonably
// type with a single dash (e.g. -exec). pflag is GNU-style and would
// otherwise treat -exec as the shorthand cluster -e with value "xec".
var longFlagNames = map[string]bool{
	"exec":    true,
	"version": true,
	"help":    true,
}

// normalizeArgs rewrites single-dash long flags to their double-dash form so
// pflag doesn't misparse them as shorthand clusters.
func normalizeArgs(args []string) []string {
	out := make([]string, 0, len(args))
	for _, arg := range args {
		if strings.HasPrefix(arg, "-") && !strings.HasPrefix(arg, "--") {
			name, _, _ := strings.Cut(strings.TrimPrefix(arg, "-"), "=")
			if longFlagNames[name] {
				arg = "-" + arg
			}
		}
		out = append(out, arg)
	}
	return out
}
