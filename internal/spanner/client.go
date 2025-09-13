package spanner

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
)

type Client struct {
	spannerClient *spanner.Client
}

func NewClient(ctx context.Context, projectID, instanceID, databaseID string) (*Client, error) {
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
