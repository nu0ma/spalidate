package spanner

import (
	"context"
	"fmt"
	"os"

	"cloud.google.com/go/spanner"
)

type Client struct {
	spannerClient *spanner.Client
}

type Options struct {
	EmulatorHost string
}

func NewClient(ctx context.Context, projectID, instanceID, databaseID string, opts ...Options) (*Client, error) {
	if len(opts) > 0 && opts[0].EmulatorHost != "" {
		if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
			if err := os.Setenv("SPANNER_EMULATOR_HOST", opts[0].EmulatorHost); err != nil {
				return nil, fmt.Errorf("failed to set SPANNER_EMULATOR_HOST: %w", err)
			}
		}
	}

	spannerClient, err := spanner.NewClient(ctx, fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID))
	if err != nil {
		return nil, err
	}
	return &Client{spannerClient: spannerClient}, err
}

func (c *Client) Query(ctx context.Context, sql string) *spanner.RowIterator {
	stmt := spanner.Statement{SQL: sql}
	return c.spannerClient.Single().Query(ctx, stmt)
}

func (c *Client) Close() {
	c.spannerClient.Close()
}
