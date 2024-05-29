package main

import (
	"github.com/spf13/pflag"
)

var (
	fExec    = pflag.StringP("exec", "e", "", "execute command and exit")
	fVersion = pflag.BoolP("version", "v", false, "print version and exit")
	fHelp    = pflag.BoolP("help", "h", false, "print help")
)
