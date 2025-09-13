package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/nu0ma/spalidate/internal/config"
	"github.com/nu0ma/spalidate/internal/logging"
	"github.com/nu0ma/spalidate/internal/spanner"
	"github.com/nu0ma/spalidate/internal/validator"
	"github.com/spf13/cobra"
)

const version = "v1.0.0"

var (
	project  string
	instance string
	database string
	port     int
	verbose  bool
	cleanup  func()
)

var rootCmd = &cobra.Command{
	Use:   "spalidate [config-file]",
	Short: "Validate Google Cloud Spanner data against YAML configuration",
	Long: `Spalidate is a CLI tool for validating Google Cloud Spanner database data 
against YAML configuration files. It connects to Spanner emulator instances 
and performs comprehensive data validation with flexible type comparison.`,
	Args:          cobra.ExactArgs(1),
	Version:       version,
	SilenceUsage:  true,
	SilenceErrors: true,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		c, err := logging.Init(verbose)
		if err != nil {
			return err
		}
		cleanup = c
		return nil
	},
	RunE: run,
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&project, "project", "p", "", "Spanner project ID (required)")
	rootCmd.PersistentFlags().StringVarP(&instance, "instance", "i", "", "Spanner instance ID (required)")
	rootCmd.PersistentFlags().StringVarP(&database, "database", "d", "", "Spanner database ID (required)")
	rootCmd.PersistentFlags().IntVar(&port, "port", 9010, "Spanner emulator port")
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Enable verbose logging (sets level=debug)")

	if err := rootCmd.MarkPersistentFlagRequired("project"); err != nil {
		panic(fmt.Sprintf("failed to mark project flag as required: %v", err))
	}
	if err := rootCmd.MarkPersistentFlagRequired("instance"); err != nil {
		panic(fmt.Sprintf("failed to mark instance flag as required: %v", err))
	}
	if err := rootCmd.MarkPersistentFlagRequired("database"); err != nil {
		panic(fmt.Sprintf("failed to mark database flag as required: %v", err))
	}
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	configPath := args[0]
	if cleanup != nil {
		defer cleanup()
	}
	logging.L().Info("Starting spalidate validation",
		"config", configPath,
		"project", project,
		"instance", instance,
		"database", database,
		"port", port,
	)

	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	logging.L().Debug("Loaded config", "tables", len(cfg.Tables))

	opts := spanner.Options{}
	if port != 0 && os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		opts.EmulatorHost = fmt.Sprintf("localhost:%d", port)
	}

	spannerClient, err := spanner.NewClient(ctx, project, instance, database, opts)
	if err != nil {
		return fmt.Errorf("creating spanner client: %w", err)
	}

	v := validator.NewValidator(cfg, spannerClient)
	if err := v.Validate(); err != nil {
		logging.L().Error("Validation failed", "error", err)
		return fmt.Errorf("validation failed: %w", err)
	}
	logging.L().Info("Validation completed successfully")

	fmt.Println("Validation passed for all tables")
	return nil
}
