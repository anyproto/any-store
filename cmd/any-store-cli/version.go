package main

import (
	"fmt"
	"runtime/debug"
)

func printVersion() {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		fmt.Println("any-store version: unknown")
		fmt.Println("any-store-cli version: unknown")
		return
	}

	cliVersion := info.Main.Version
	anyStoreVersion := "unknown"

	for _, dep := range info.Deps {
		if dep.Path == "github.com/anyproto/any-store" {
			anyStoreVersion = dep.Version
			if dep.Replace != nil {
				if dep.Replace.Version != "" {
					anyStoreVersion = dep.Replace.Version
				} else {
					anyStoreVersion = "(devel)"
				}
			}
			break
		}
	}

	fmt.Printf("any-store version: %s\n", anyStoreVersion)
	fmt.Printf("any-store-cli version: %s\n", cliVersion)
}
