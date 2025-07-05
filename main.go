package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/nu0ma/spalidate/internal/config"
	"github.com/nu0ma/spalidate/internal/spanner"
	"github.com/nu0ma/spalidate/internal/validator"
)

const version = "v1.0.0"

func main() {
	var (
		projectID  = flag.String("project-id", "", "Google Cloud Project ID")
		configPath = flag.String("config", "", "Path to YAML configuration file")
		showVersion = flag.Bool("version", false, "Show version information")
		verbose    = flag.Bool("verbose", false, "Enable verbose logging")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("spalidate %s\n", version)
		return
	}

	if *projectID == "" {
		fmt.Fprintf(os.Stderr, "Error: --project-id is required\n")
		flag.Usage()
		os.Exit(1)
	}

	if *configPath == "" {
		fmt.Fprintf(os.Stderr, "Error: --config is required\n")
		flag.Usage()
		os.Exit(1)
	}

	if *verbose {
		log.SetFlags(log.LstdFlags | log.Lshortfile)
		log.Println("Starting spalidate validation")
	}

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
		os.Exit(1)
	}

	if *verbose {
		log.Printf("Loaded config with %d tables", len(cfg.Tables))
	}

	spannerClient, err := spanner.NewClient(*projectID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating Spanner client: %v\n", err)
		os.Exit(1)
	}
	defer spannerClient.Close()

	if *verbose {
		log.Println("Connected to Spanner")
	}

	validator := validator.New(spannerClient)
	
	results, err := validator.Validate(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error during validation: %v\n", err)
		os.Exit(1)
	}

	if results.HasErrors() {
		fmt.Println("Validation failed:")
		for _, err := range results.Errors {
			fmt.Printf("  ❌ %s\n", err)
		}
		os.Exit(1)
	}

	fmt.Println("✅ All validations passed!")
	if *verbose {
		for _, msg := range results.Messages {
			fmt.Printf("  ✓ %s\n", msg)
		}
	}
}