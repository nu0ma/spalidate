package cmd

import (
	"fmt"
	"log"
	"os"

	"github.com/nu0ma/spalidate/internal/config"
	"github.com/spf13/cobra"
)

const version = "v1.0.0"

var (
	project  string
	instance string
	database string
	port     int
	verbose  bool
)

var rootCmd = &cobra.Command{
	Use:   "spalidate [config-file]",
	Short: "Validate Google Cloud Spanner data against YAML configuration",
	Long: `Spalidate is a CLI tool for validating Google Cloud Spanner database data 
against YAML configuration files. It connects to Spanner emulator instances 
and performs comprehensive data validation with flexible type comparison.`,
	Args:    cobra.ExactArgs(1),
	Version: version,
	RunE:    run,
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&project, "project", "p", "", "Spanner project ID (required)")
	rootCmd.PersistentFlags().StringVarP(&instance, "instance", "i", "", "Spanner instance ID (required)")
	rootCmd.PersistentFlags().StringVarP(&database, "database", "d", "", "Spanner database ID (required)")
	rootCmd.PersistentFlags().IntVar(&port, "port", 9010, "Spanner emulator port")
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Enable verbose logging")

	rootCmd.MarkPersistentFlagRequired("project")
	rootCmd.MarkPersistentFlagRequired("instance")
	rootCmd.MarkPersistentFlagRequired("database")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, args []string) error {
	configPath := args[0]

	if verbose {
		log.SetFlags(log.LstdFlags | log.Lshortfile)
		log.Println("Starting spalidate validation")
	}

	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	if verbose {
		log.Printf("Loaded config with %d tables", len(cfg.Tables))
	}

	return nil
}
