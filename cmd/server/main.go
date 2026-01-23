package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/nickyhof/CommitDB"
	"github.com/nickyhof/CommitDB/core"
	"github.com/nickyhof/CommitDB/ps"
)

// Version is set at build time via -ldflags
var Version = "dev"

func main() {
	port := flag.Int("port", 3306, "TCP port to listen on")
	baseDir := flag.String("baseDir", "", "Base directory for persistence (memory if empty)")
	gitUrl := flag.String("gitUrl", "", "Git URL for remote sync")
	showVersion := flag.Bool("version", false, "Show version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("CommitDB SQL Server v%s\n", Version)
		return
	}

	// Initialize persistence
	var instance *CommitDB.Instance
	if *baseDir == "" {
		log.Println("Using memory persistence")
		persistence, err := ps.NewMemoryPersistence()
		if err != nil {
			log.Fatalf("Failed to initialize memory persistence: %v", err)
		}
		instance = CommitDB.Open(&persistence)
	} else {
		log.Printf("Using file persistence: %s", *baseDir)
		var gitUrlPtr *string
		if *gitUrl != "" {
			gitUrlPtr = gitUrl
		}
		persistence, err := ps.NewFilePersistence(*baseDir, gitUrlPtr)
		if err != nil {
			log.Fatalf("Failed to initialize file persistence: %v", err)
		}
		instance = CommitDB.Open(&persistence)
	}

	// Create and start server
	identity := core.Identity{
		Name:  "CommitDB Server",
		Email: "server@commitdb.local",
	}

	server := NewServer(instance, identity)
	addr := fmt.Sprintf(":%d", *port)

	if err := server.Start(addr); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	// Print banner
	fmt.Println()
	bannerWidth := 39 // inner width of the banner box
	versionLine := fmt.Sprintf("CommitDB SQL Server v%s", Version)
	padding := bannerWidth - len(versionLine) - 2 // -2 for "  " margins
	if padding < 0 {
		padding = 0
	}
	leftPad := padding / 2
	rightPad := padding - leftPad

	fmt.Println("╔═══════════════════════════════════════╗")
	fmt.Printf("║ %*s%s%*s ║\n", leftPad, "", versionLine, rightPad, "")
	fmt.Println("║   Git-backed SQL Database Engine      ║")
	fmt.Println("╚═══════════════════════════════════════╝")
	fmt.Println()
	fmt.Printf("Listening on port %d\n", *port)
	fmt.Println("Send SQL queries (one per line), 'quit' to disconnect")
	fmt.Println()

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")
	server.Stop()
	log.Println("Server stopped")
}
