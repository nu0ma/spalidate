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
	"go.uber.org/zap"
)

const version = "v1.0.0"

var (
	project   string
	instance  string
	database  string
	port      int
	verbose   bool
	logLevel  string
	logFormat string
	cleanup   func()
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
		c, err := logging.Init(logLevel, logFormat, verbose)
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
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "info", "Log level: debug, info, warn, error")
	rootCmd.PersistentFlags().StringVar(&logFormat, "log-format", "console", "Log format: console or json")

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
	ctx := context.Background()
	configPath := args[0]
	if cleanup != nil {
		defer cleanup()
	}
	zap.L().Info("Starting spalidate validation",
		zap.String("config", configPath),
		zap.String("project", project),
		zap.String("instance", instance),
		zap.String("database", database),
		zap.Int("port", port),
	)

	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	zap.L().Debug("Loaded config",
		zap.Int("tables", len(cfg.Tables)),
	)

	spannerClient, err := spanner.NewClient(ctx, project, instance, database)
	if err != nil {
		return fmt.Errorf("creating spanner client: %w", err)
	}

    v := validator.NewValidator(cfg, spannerClient)
    if err := v.Validate(); err != nil {
        // 失敗詳細はERRORで出力（テストが期待する文言も含む）
        zap.L().Error("Validation failed", zap.Error(err))
        return fmt.Errorf("validation failed: %w", err)
    }
	zap.L().Info("Validation completed successfully")
	fmt.Println("Validation passed for all tables")
	return nil
}
