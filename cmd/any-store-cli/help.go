package main

import (
	"fmt"
	"os"

	"github.com/spf13/pflag"
)

func printUsage() {
	fmt.Fprintf(os.Stderr, "Usage:\n%s path/to/dbFile.db  [flags]\nFlags:\n", os.Args[0])
	pflag.PrintDefaults()
}
