package snowflake

import (
	"context"
	"fmt"
	"github.com/aaronland/go-uid"
	"testing"
)

func TestSnowflakeProvider(t *testing.T) {

	ctx := context.Background()

	uri := fmt.Sprintf("%s://", SNOWFLAKE_SCHEME)

	pr, err := uid.NewProvider(ctx, uri)

	if err != nil {
		t.Fatalf("Failed to create provider for %s, %v", uri, err)
	}

	id, err := pr.UID(ctx)

	if err != nil {
		t.Fatalf("Failed to create snowflake UID, %v", err)
	}

	if id.String() == "0" {
		t.Fatalf("Invalid ID for snowflake provider")
	}

	_, ok := uid.AsInt64(id)

	if !ok {
		t.Fatalf("Expected value to be int64")
	}
}
