package parser

import (
	"github.com/PuerkitoBio/goquery"
	"github.com/zzayne/go-crawler/engine"
	"github.com/zzayne/go-crawler/model"
)

// Parser 通用解析器接口
type Parser interface {
	Parse(doc *goquery.Document) (engine.ParseResult, error)
}

// ItemParser 通用的数据项解析器
type ItemParser struct {
	// CSS选择器配置
	TitleSelector   string
	ContentSelector string
	LinkSelector    string
	// 属性选择器映射
	AttributeSelectors map[string]string
}

// Parse 解析页面
func (p *ItemParser) Parse(doc *goquery.Document) (engine.ParseResult, error) {
	result := engine.ParseResult{}
	url := doc.Url.String()
	
	// 解析标题
	title := doc.Find(p.TitleSelector).Text()
	
	// 解析内容
	content := doc.Find(p.ContentSelector).Text()
	
	// 解析属性
	attributes := make(map[string]interface{})
	for key, selector := range p.AttributeSelectors {
		attributes[key] = doc.Find(selector).Text()
	}
	
	// 创建数据项
	item := engine.Item{
		URL:  url,
		Type: "item",
		Payload: model.Item{
			URL:        url,
			Title:      title,
			Content:    content,
			Attributes: attributes,
		},
	}
	
	result.Items = append(result.Items, item)
	
	// 解析后续链接
	doc.Find(p.LinkSelector).Each(func(i int, s *goquery.Selection) {
		link, exists := s.Attr("href")
		if exists {
			result.Requests = append(result.Requests, engine.Request{
				URL:       link,
				ParseFunc: p.Parse,
			})
		}
	})
	
	return result, nil
}

// ListParser 列表页解析器
type ListParser struct {
	ItemSelector string
	NextPageSelector string
	ItemParser Parser
}

// Parse 解析列表页
func (p *ListParser) Parse(doc *goquery.Document) (engine.ParseResult, error) {
	result := engine.ParseResult{}
	
	// 解析列表项
	doc.Find(p.ItemSelector).Each(func(i int, s *goquery.Selection) {
		link, exists := s.Attr("href")
		if exists {
			result.Requests = append(result.Requests, engine.Request{
				URL:       link,
				ParseFunc: p.ItemParser.Parse,
			})
		}
	})
	
	// 解析下一页
	nextPage, exists := doc.Find(p.NextPageSelector).Attr("href")
	if exists {
		result.Requests = append(result.Requests, engine.Request{
			URL:       nextPage,
			ParseFunc: p.Parse,
		})
	}
	
	return result, nil
}