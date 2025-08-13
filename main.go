package main

import (
	"context"
	"log"
	"strconv"

	"github.com/zzayne/go-crawler/engine"
	parser "github.com/zzayne/go-crawler/parser/lianjia"
	"github.com/zzayne/go-crawler/persist"
	"github.com/zzayne/go-crawler/scheduler"
)

func main() {
	ctx := context.Background()
	itemChan, err := persist.ItemSaver(ctx, "lianjia")
	if err != nil {
		log.Fatal(err)
	}

	e := engine.Engine{
		WorkerCount: 10,
		Scheduler:   &scheduler.QueueScheduler{},
		ItemChan:    itemChan,
	}

	var reqList []engine.Request

	//链家的租房无明显入口，每页内容也无较好方法提取的下一页url，所以先手动提供120个分页入口来爬取内容
	for i := 1; i <= 120; i++ {
		req := engine.Request{
			URL:       "https://sz.lianjia.com/zufang/pg" + strconv.Itoa(i),
			ParseFunc: parser.RentListParser,
		}
		reqList = append(reqList, req)
	}

	e.Run(reqList...)
}
