package main

import (
	"context"

	"github.com/sarzhann/clarify-inserter/inserter"
)

func main() {
	ctx := context.Background()
	defer ctx.Done()

	inserter, err := inserter.New(ctx)
	if err != nil {
		panic(err)
	}

	inserter.Run(ctx)
}
