package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/aaronland/go-uid"
	_ "github.com/aaronland/go-uid-snowflake"
	"log"
)

func main() {

	flag.Parse()

	ctx := context.Background()
	pr, err := uid.NewProvider(ctx, "snowflake://")

	if err != nil {
		log.Fatalf("Failed to create provider, %v", err)
	}

	id, err := pr.UID(ctx)

	if err != nil {
		log.Fatalf("Failed to create UID, %v", err)
	}

	fmt.Println(id.String())
}
