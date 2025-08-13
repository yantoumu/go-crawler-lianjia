package persist

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/zzayne/go-crawler/engine"
	"gopkg.in/olivere/elastic.v5"
)

// ItemSaver creates a channel for saving items with context support
func ItemSaver(ctx context.Context, index string) (chan engine.Item, error) {
	//连接docker中的elastic时，需要SetSniff off
	client, err := elastic.NewClient(elastic.SetSniff(false))

	if err != nil {
		return nil, err
	}

	// Use buffered channel to prevent blocking
	in := make(chan engine.Item, 100)
	go func() {
		defer close(in)
		itemCount := 0
		var retryCount int
		for {
			select {
			case <-ctx.Done():
				log.Printf("Item Saver: Shutting down, processed %d items", itemCount)
				return
			case item, ok := <-in:
				if !ok {
					log.Printf("Item Saver: Channel closed, processed %d items", itemCount)
					return
				}
				log.Printf("Item Saver:Saved  item #%d:%v", itemCount, item)
				itemCount++

				// Implement retry logic for error recovery
				err = saveWithRetry(ctx, client, item, index, &retryCount)
				if err != nil {
					log.Printf("Item Saver:error saving item %v:%v", item, err)
				}
			}
		}
	}()
	return in, nil
}

// save saves an item to elasticsearch
func save(client *elastic.Client, item engine.Item, index string) error {
	if item.Type == "" {
		return errors.New("Type can't be null")
	}
	indexService := client.Index().
		Index(index).
		Type(item.Type).
		BodyJson(item)

	if item.ID != "" {
		indexService.Id(item.ID)
	}
	_, err := indexService.Do(context.Background())

	return err
}

// saveWithRetry saves an item with retry logic for error recovery
func saveWithRetry(ctx context.Context, client *elastic.Client, item engine.Item, index string, retryCount *int) error {
	const maxRetries = 3

	for attempt := 0; attempt < maxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			err := save(client, item, index)
			if err == nil {
				*retryCount = 0 // Reset retry count on success
				return nil
			}

			if attempt == maxRetries-1 {
				*retryCount++
				log.Printf("Failed to save item after %d attempts: %v", maxRetries, err)
				return err
			}

			log.Printf("Save attempt %d failed, retrying: %v", attempt+1, err)
			// Exponential backoff delay
			time.Sleep(time.Duration(attempt+1) * time.Second)
		}
	}
	return nil
}
