package main

import (
	"fmt"
	"os"

	"github.com/spf13/pflag"
)

func main() {
	if err := pflag.CommandLine.Parse(normalizeArgs(os.Args[1:])); err != nil {
		os.Exit(2)
	}
	if *fHelp {
		printHelp(os.Stdout, helpName(), os.Getenv(helpForAgentEnv) == "1")
		return
	}
	if *fVersion {
		printVersion()
		return
	}
	path := pflag.Arg(0)
	if path == "" {
		fmt.Fprintln(os.Stderr, "db file is not provided")
		printUsage()
		os.Exit(1)
	}

	if err := openConn(path); err != nil {
		fmt.Fprintf(os.Stderr, "error while opening database: %v\n", err)
		os.Exit(1)
	}

	if *fExec == "" {
		printVersion()
		if stats, err := conn.ShowStats(); err == nil {
			fmt.Print(stats)
		}
	}

	if *fExec != "" {
		// A single command has no "it" to page with: print every result.
		conn.pageSize = 0
		result, err := conn.Exec(*fExec)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error while executing command: %v\n", err)
			os.Exit(1)
		}
		if result != "" {
			fmt.Println(result)
		}
		return
	}

	runLiner()
}
