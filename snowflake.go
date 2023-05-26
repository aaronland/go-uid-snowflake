package snowflake

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"strconv"
	"sync"

	"github.com/aaronland/go-uid"
	"github.com/bwmarrin/snowflake"
)

const SNOWFLAKE_SCHEME string = "snowflake"

var nodes *sync.Map

func init() {
	ctx := context.Background()
	uid.RegisterProvider(ctx, SNOWFLAKE_SCHEME, NewSnowflakeProvider)

	nodes = new(sync.Map)
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

	u, err := url.Parse(uri)

	if err != nil {
		return nil, fmt.Errorf("Failed to parse URI, %w", err)
	}

	q := u.Query()

	var node_id int64

	q_node_id := q.Get("node-id")

	if q_node_id != "" {

		v, err := strconv.ParseInt(q_node_id, 10, 64)

		if err != nil {
			return nil, fmt.Errorf("Failed to parse ?node-id= paramterer, %w", err)
		}

		node_id = v

	} else {

		for i := 0; i < 1023; i++ {

			node_id = rand.Int63n(1023)

			_, exists := nodes.Load(node_id)

			if !exists {
				break
			}
		}

		if node_id == 0 {
			return nil, fmt.Errorf("Failed to derive unique node ID")
		}

		nodes.Store(node_id, true)
	}

	node, err := snowflake.NewNode(node_id)

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

func (pr *SnowflakeProvider) SetLogger(ctx context.Context, logger *log.Logger) error {
	return nil
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
