package main

import (
	"log"

	"github.com/go-ego/riot"
	"github.com/go-ego/riot/types"
)

var (
	searcher = riot.New("../../data/dict/dictionary.txt")
)

func main() {
	data := types.DocIndexData{Content: "留给真爱你的人"}
	searcher.IndexDoc(1, data)
	searcher.IndexDoc(2, data)
	searcher.FlushIndex()

	req := types.SearchReq{Text: "真爱"}
	search := searcher.Search(req)
	log.Println("search...", search)
}