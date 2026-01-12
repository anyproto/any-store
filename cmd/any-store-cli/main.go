package main

import (
	"fmt"
	"os"

	"github.com/spf13/pflag"
)

func main() {
	pflag.Parse()
	if *fHelp {
		printUsage()
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
		fmt.Fprintf(os.Stderr, "error while opening database: %v", err)
		os.Exit(1)
	}

	if *fExec == "" {
		printVersion()
		if stats, err := conn.ShowStats(); err == nil {
			fmt.Print(stats)
		}
	}

	if *fExec != "" {
		result, err := conn.Exec(*fExec)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error while executiong command: %v", err)
			os.Exit(1)
		}
		fmt.Println(result)
		return
	}

	runLiner()
}
