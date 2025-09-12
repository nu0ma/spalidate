package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/nu0ma/spalidate/internal/config"
	"github.com/nu0ma/spalidate/internal/validator"
)

const version = "v1.0.0"

type flags struct {
	project  string
	instance string
	database string
	port     int
	version  bool
	verbose  bool
}

func main() {
	f := parseFlags()
	
	if f.version {
		fmt.Printf("spalidate %s\n", version)
		return
	}

	configPath := validateArgs(f)
	
	if f.verbose {
		log.SetFlags(log.LstdFlags | log.Lshortfile)
		log.Println("Starting spalidate validation")
	}

	cfg, err := config.LoadConfig(configPath)
	exitOnError(err, "loading config")
	
	if f.verbose {
		log.Printf("Loaded config with %d tables", len(cfg.Tables))
	}

	validator, err := validator.NewValidator(f.project, f.instance, f.database, f.port)
	exitOnError(err, "creating validator")
	defer validator.Close()

	if f.verbose {
		log.Println("Connected to Spanner")
	}

	results, err := validator.Validate(cfg)
	exitOnError(err, "during validation")

	printResults(results, f.verbose)
}

func parseFlags() flags {
	f := flags{}
	flag.StringVar(&f.project, "project", "", "Spanner project ID")
	flag.StringVar(&f.instance, "instance", "", "Spanner instance ID")
	flag.StringVar(&f.database, "database", "", "Spanner database ID")
	flag.IntVar(&f.port, "port", 9010, "Spanner emulator port")
	flag.BoolVar(&f.version, "version", false, "Show version information")
	flag.BoolVar(&f.verbose, "verbose", false, "Enable verbose logging")
	flag.Parse()
	return f
}

func validateArgs(f flags) string {
	requiredFlags := map[string]string{
		"project":  f.project,
		"instance": f.instance,
		"database": f.database,
	}
	
	for name, value := range requiredFlags {
		if value == "" {
			exitWithUsage("Error: --%s is required\n", name)
		}
	}

	args := flag.Args()
	if len(args) != 1 {
		exitWithUsage("Error: config file path is required as positional argument\n")
	}
	
	return args[0]
}

func exitWithUsage(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format, args...)
	flag.Usage()
	os.Exit(1)
}

func exitOnError(err error, context string) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error %s: %v\n", context, err)
		os.Exit(1)
	}
}

func printResults(results *validator.ValidationResult, verbose bool) {
	if results.HasErrors() {
		fmt.Println("Validation failed:")
		for _, err := range results.Errors {
			fmt.Printf("  ❌ %s\n", err)
		}
		os.Exit(1)
	}

	fmt.Println("✅ All validations passed!")
	if verbose {
		for _, msg := range results.Messages {
			fmt.Printf("  ✓ %s\n", msg)
		}
	}
}