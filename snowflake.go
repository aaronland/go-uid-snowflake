package snowflake

import (
	"context"
	"fmt"
	"github.com/aaronland/go-uid"
	"github.com/bwmarrin/snowflake"
)

const SNOWFLAKE_SCHEME string = "snowflake"

func init() {
	ctx := context.Background()
	uid.RegisterProvider(ctx, SNOWFLAKE_SCHEME, NewSnowflakeProvider)
}

type SnowflakeProvider struct {
	uid.Provider
	node *snowflake.Node
}

type SnowflakeUID struct {
	uid.UID
	id int64
}

func NewSnowflakeProvider(ctx context.Context, uri string) (uid.Provider, error) {

	node, err := snowflake.NewNode(1)

	if err != nil {
		return nil, fmt.Errorf("Failed to generate snowflake node, %w", err)
	}

	pr := &SnowflakeProvider{
		node: node,
	}

	return pr, nil
}

func (pr *SnowflakeProvider) UID(ctx context.Context, args ...interface{}) (uid.UID, error) {
	return NewSnowflakeUID(ctx, pr.node)
}

func NewSnowflakeUID(ctx context.Context, args ...interface{}) (uid.UID, error) {

	if len(args) != 1 {
		return nil, fmt.Errorf("Invalid arguments")
	}

	n, ok := args[0].(*snowflake.Node)

	if !ok {
		return nil, fmt.Errorf("Invalid node")
	}

	id := n.Generate()

	u := &SnowflakeUID{
		id: id.Int64(),
	}

	return u, nil
}

func (u *SnowflakeUID) Value() any {
	return u.id
}

func (u *SnowflakeUID) String() string {
	return fmt.Sprintf("%v", u.Value())
}
