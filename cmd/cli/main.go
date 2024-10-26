package main

import (
	"LogDb/internal/adapters/datastor"
	"LogDb/internal/adapters/index"
	"LogDb/internal/adapters/presenters"
	"LogDb/internal/adapters/query"
	"LogDb/internal/adapters/serializer"
	"LogDb/internal/domain/query_types"
	"LogDb/pkg/utils/usage"
	"fmt"
	"log"
	"time"
)

func init() {
	go func() {
		for {
			usage.MemoryUsage()
			time.Sleep(1 * time.Second)
		}
	}()
}

const BaseDir = ".storage"

func main() {
	codec := serializer.Default
	idx := index.NewTimestamp(BaseDir, codec)
	stor := datastor.NewPersistentStorage(BaseDir, codec, idx)
	render := presenters.NewQueryResultPresenter(presenters.NewLogRecordRawStringPresenter())
	defer stor.Close()
	builder := query.NewQueryBuilder(query_types.Select, "audit", "logs").
		SetPartition("shard1").
		SetTimeRange(time.Now().Add(-24*time.Hour), time.Now()).
		Where("label.foo", query_types.Exists, nil).
		Where("label.bar", query_types.IsNotNull, nil).
		Where("label.baz", query_types.Equal, "fiz").
		Limit(1000).
		AggregateBy(query_types.Minute).
		SetFormat(query_types.JSON)
	query := builder.Build()
	log.Printf("Executing query: %s", query)
	result, err := stor.Query(&query)
	if err != nil {
		panic(err)
	}
	fmt.Println(render.Present(result))
}
