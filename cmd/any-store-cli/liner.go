package main

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/peterh/liner"
)

var linerContext = "> "

func runLiner() {
	line := liner.NewLiner()
	line.SetCompleter(conn.Complete)
	line.SetCtrlCAborts(true)
	lineClose := func() {
		mainCtx.CancelAndReplace()
		_ = line.Close()
		_ = conn.db.Close()
	}
	defer lineClose()

	for {
		cmdLine, err := line.Prompt(linerContext)
		if err != nil {
			if errors.Is(err, liner.ErrPromptAborted) {
				fmt.Fprintln(os.Stderr, "close database..")
				lineClose()
				fmt.Fprintln(os.Stderr, "goodbuy!")
				os.Exit(0)
			} else {
				lineClose()
				fmt.Fprintf(os.Stderr, "unexpected error: %v\n", err)
				os.Exit(1)
			}
		}
		cmdLine = strings.TrimSpace(cmdLine)
		if cmdLine == "" {
			continue
		}
		line.AppendHistory(cmdLine)

		st := time.Now()
		if res, err := conn.Exec(cmdLine); err != nil {
			fmt.Fprintf(os.Stderr, "exec error: %v\n\n%s", err, res)
		} else {
			if res != "" {
				fmt.Println(res)
			}
			fmt.Fprintf(os.Stderr, "%s %v\n", color.MagentaString("ok"), time.Since(st))
		}
	}
}
