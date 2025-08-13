package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"
	
	"github.com/zzayne/go-crawler/engine"
	"github.com/zzayne/go-crawler/model"
)

// ItemSaver 创建PostgreSQL版本的ItemSaver
func ItemSaver(ctx context.Context, db *DB) (chan engine.Item, error) {
	in := make(chan engine.Item, 100)
	
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("ItemSaver panic recovered: %v", r)
			}
		}()
		
		itemCount := 0
		for {
			select {
			case <-ctx.Done():
				log.Printf("ItemSaver: Shutting down, saved %d items", itemCount)
				return
			case item, ok := <-in:
				if !ok {
					log.Printf("ItemSaver: Channel closed, saved %d items", itemCount)
					return
				}
				
				// 保存到PostgreSQL
				if err := saveItem(db, item); err != nil {
					log.Printf("Error saving item: %v", err)
					continue
				}
				
				itemCount++
				log.Printf("Item #%d saved: %v", itemCount, item)
			}
		}
	}()
	
	return in, nil
}

// saveItem 保存单个项目到数据库
func saveItem(db *DB, item engine.Item) error {
	// 类型断言，获取具体的Item
	var crawledItem model.Item
	
	switch v := item.Payload.(type) {
	case model.Item:
		crawledItem = v
	default:
		// 如果不是model.Item类型，尝试创建一个通用的Item
		crawledItem = model.Item{
			URL:       item.URL,
			Type:      item.Type,
			Title:     fmt.Sprintf("%v", item.Payload),
			CrawledAt: time.Now(),
			UpdatedAt: time.Now(),
		}
	}
	
	// 序列化属性为JSON
	attributesJSON, err := json.Marshal(crawledItem.Attributes)
	if err != nil {
		return fmt.Errorf("failed to marshal attributes: %w", err)
	}
	
	// 插入或更新数据
	query := `
		INSERT INTO crawled_items (url, type, title, content, attributes, crawled_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (url) 
		DO UPDATE SET 
			type = EXCLUDED.type,
			title = EXCLUDED.title,
			content = EXCLUDED.content,
			attributes = EXCLUDED.attributes,
			updated_at = EXCLUDED.updated_at
		RETURNING id`
	
	var id int64
	err = db.QueryRow(
		query,
		crawledItem.URL,
		crawledItem.Type,
		crawledItem.Title,
		crawledItem.Content,
		attributesJSON,
		crawledItem.CrawledAt,
		crawledItem.UpdatedAt,
	).Scan(&id)
	
	if err != nil {
		return fmt.Errorf("failed to save item: %w", err)
	}
	
	crawledItem.ID = id
	return nil
}

// ItemDAO 数据访问对象
type ItemDAO struct {
	db *DB
}

// NewItemDAO 创建新的DAO
func NewItemDAO(db *DB) *ItemDAO {
	return &ItemDAO{db: db}
}

// Save 保存项目
func (dao *ItemDAO) Save(item model.Item) (int64, error) {
	attributesJSON, err := json.Marshal(item.Attributes)
	if err != nil {
		return 0, err
	}
	
	query := `
		INSERT INTO crawled_items (url, type, title, content, attributes, crawled_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (url) 
		DO UPDATE SET 
			type = EXCLUDED.type,
			title = EXCLUDED.title,
			content = EXCLUDED.content,
			attributes = EXCLUDED.attributes,
			updated_at = EXCLUDED.updated_at
		RETURNING id`
	
	var id int64
	err = dao.db.QueryRow(
		query,
		item.URL,
		item.Type,
		item.Title,
		item.Content,
		attributesJSON,
		item.CrawledAt,
		item.UpdatedAt,
	).Scan(&id)
	
	return id, err
}

// FindByURL 根据URL查找
func (dao *ItemDAO) FindByURL(url string) (*model.Item, error) {
	query := `
		SELECT id, url, type, title, content, attributes, crawled_at, updated_at
		FROM crawled_items
		WHERE url = $1`
	
	var item model.Item
	var attributesJSON []byte
	
	err := dao.db.QueryRow(query, url).Scan(
		&item.ID,
		&item.URL,
		&item.Type,
		&item.Title,
		&item.Content,
		&attributesJSON,
		&item.CrawledAt,
		&item.UpdatedAt,
	)
	
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	
	if err := json.Unmarshal(attributesJSON, &item.Attributes); err != nil {
		return nil, err
	}
	
	return &item, nil
}

// FindByType 根据类型查找
func (dao *ItemDAO) FindByType(itemType string, limit int) ([]*model.Item, error) {
	query := `
		SELECT id, url, type, title, content, attributes, crawled_at, updated_at
		FROM crawled_items
		WHERE type = $1
		ORDER BY crawled_at DESC
		LIMIT $2`
	
	rows, err := dao.db.Query(query, itemType, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	
	var items []*model.Item
	for rows.Next() {
		var item model.Item
		var attributesJSON []byte
		
		err := rows.Scan(
			&item.ID,
			&item.URL,
			&item.Type,
			&item.Title,
			&item.Content,
			&attributesJSON,
			&item.CrawledAt,
			&item.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		
		if err := json.Unmarshal(attributesJSON, &item.Attributes); err != nil {
			return nil, err
		}
		
		items = append(items, &item)
	}
	
	return items, nil
}