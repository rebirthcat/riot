// Copyright 2017 ego authors
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package riot

import (

	"log"
	"os"
	"strings"

	"github.com/rebirthcat/riot/core"
	"github.com/rebirthcat/riot/types"
	toml "github.com/go-vgo/gt/conf"
)

// New create a new engine with mode
func New(conf ...interface{}) *Engine {
	// func (engine *Engine) New(conf com.Config) *Engine{
	if len(conf) > 0 && strings.HasSuffix(conf[0].(string), ".toml") {
		var (
			config   types.EngineOpts
			searcher = &Engine{}
		)

		fs := conf[0].(string)
		log.Println("conf path is: ", fs)
		toml.Init(fs, &config)
		go toml.Watch(fs, &config)

		searcher.Init(config)
		return searcher
	}

	return NewEngine(conf...)
}

// NewEngine create a new engine
func NewEngine(conf ...interface{}) *Engine {
	var (
		searcher = &Engine{}

		path          = DefaultPath
		storageShards = 10
		numShards     = 10

		segmentDict string
	)

	if len(conf) > 0 {
		segmentDict = conf[0].(string)
	}

	if len(conf) > 1 {
		path = conf[1].(string)
	}

	if len(conf) > 2 {
		numShards = conf[2].(int)
		storageShards = conf[2].(int)
	}

	searcher.Init(types.EngineOpts{
		// Using:         using,
		StoreShards: storageShards,
		NumShards:   numShards,
		IndexerOpts: &types.IndexerOpts{
			IndexType: types.DocIdsIndex,
		},
		UseStore:    true,
		StoreFolder: path,
		// StoreEngine: storageEngine,
		GseDict: segmentDict,
		// StopTokenFile: stopTokenFile,
	})

	// defer searcher.Close()
	os.MkdirAll(path, 0777)

	// 等待索引刷新完毕
	// searcher.Flush()
	// log.Println("recover index number: ", searcher.NumDocsIndexed())

	return searcher
}

// HasDoc if the document is exist return true
func (engine *Engine) HasDoc(docId string) bool {
	for shard := 0; shard < engine.initOptions.NumShards; shard++ {
		engine.indexers = append(engine.indexers, &core.Indexer{})

		has := engine.indexers[shard].HasDoc(docId)

		if has {
			return true
		}
	}

	return false
}

//得到当前内存中被索引的文档数量
func (engine *Engine)GetNumDocsIndexed() uint64 {
	var num uint64
	for shard := 0; shard < engine.initOptions.NumShards; shard++ {
		num+=engine.indexers[shard].GetNumDocs()
	}
	return num
}
//得到内存中所有关键词数量（有重复）
func (engine *Engine)GetNumTokenIndexAdded() uint64 {
	var num uint64
	for shard := 0; shard < engine.initOptions.NumShards; shard++ {
		num+=engine.indexers[shard].GetNumTotalTokenLen()
	}
	return num
}

//得到当前持久化数据库中有多少被索引的文档的数量
func (engine *Engine)GetNumDocsStored() uint64  {
	var num uint64
	for shard := 0; shard < engine.initOptions.NumShards; shard++ {
		num+=engine.indexers[shard].GetNumDocsStore()
	}
	return num
}
//得到关键词总数（不重复）
func (engine *Engine)GetNumReverseTableLen()uint64  {
	var num uint64
	for shard := 0; shard < engine.initOptions.NumShards; shard++ {
		num+=engine.indexers[shard].GetTableLen()
	}
	return num
}

