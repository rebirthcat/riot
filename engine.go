// Copyright 2013 Hui Chen
// Copyright 2016 ego authors
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

/*

Package riot is riot engine
*/
package riot

import (
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rebirthcat/riot/core"
	"github.com/rebirthcat/riot/types"
	"github.com/rebirthcat/riot/utils"

	"github.com/go-ego/gse"
	"github.com/go-ego/murmur"
	"github.com/shirou/gopsutil/mem"
)

const (
	// Version get the riot version
	Version string = "v0.10.0.425, Danube River!"

	// NumNanosecondsInAMillisecond nano-seconds in a milli-second num
	NumNanosecondsInAMillisecond = 1000000
	// StoreFilePrefix persistent store file prefix
	StoreFilePrefix = "riot"

	// DefaultPath default db path
	DefaultPath = "./riot-index"
)

// GetVersion get the riot version
func GetVersion() string {
	return Version
}

// Engine initialize the engine
type Engine struct {
	//lock  sync.RWMutex

	// 计数器，用来统计有多少文档被索引等信息
	numDocsIndexed      uint64
	numTokenIndexAdded   uint64
	numDocsStored        uint64
	isFlushing          int

	// 记录初始化参数
	initOptions types.EngineOpts
	initialized bool

	indexers   []*core.Indexer
	rankers    []*core.Ranker
	segmenter  gse.Segmenter
	loaded     bool
	stopTokens StopTokens
	// 建立索引器使用的通信通道
	segmenterChan         chan segmenterReq
	indexerAddDocChans    []chan indexerAddDocReq
	indexerRemoveDocChans []chan indexerRemoveDocReq

	// 建立排序器使用的通信通道
	indexerLookupChans   []chan indexerLookupReq
	rankerRankChans      []chan rankerRankReq
}


// Indexer initialize the indexer channel
func (engine *Engine) Indexer(options types.EngineOpts) {
	engine.indexerAddDocChans = make(
		[]chan indexerAddDocReq, options.NumShards)

	engine.indexerRemoveDocChans = make(
		[]chan indexerRemoveDocReq, options.NumShards)

	engine.indexerLookupChans = make(
		[]chan indexerLookupReq, options.NumShards)

	for shard := 0; shard < options.NumShards; shard++ {
		engine.indexerAddDocChans[shard] = make(
			chan indexerAddDocReq, options.IndexerBufLen)

		engine.indexerRemoveDocChans[shard] = make(
			chan indexerRemoveDocReq, options.IndexerBufLen)

		engine.indexerLookupChans[shard] = make(
			chan indexerLookupReq, options.IndexerBufLen)
	}
}

// Ranker initialize the ranker channel
func (engine *Engine) Ranker(options types.EngineOpts) {
	engine.rankerRankChans = make(
		[]chan rankerRankReq, options.NumShards)

	for shard := 0; shard < options.NumShards; shard++ {
		engine.rankerRankChans[shard] = make(
			chan rankerRankReq, options.RankerBufLen)
	}
}



// CheckMem check the memory when the memory is larger
// than 99.99% using the store
func (engine *Engine) CheckMem() {
	// Todo test
	if !engine.initOptions.UseStore {
		log.Println("Check virtualMemory...")

		vmem, _ := mem.VirtualMemory()
		log.Printf("Total: %v, Free: %v, UsedPercent: %f%%\n",
			vmem.Total, vmem.Free, vmem.UsedPercent)

		useMem := fmt.Sprintf("%.2f", vmem.UsedPercent)
		if useMem == "99.99" {
			engine.initOptions.UseStore = true
			engine.initOptions.StoreFolder = DefaultPath
			// os.MkdirAll(DefaultPath, 0777)
		}
	}
}



// WithGse Using user defined segmenter
// If using a not nil segmenter and the dictionary is loaded,
// the `opt.GseDict` will be ignore.
func (engine *Engine) WithGse(segmenter gse.Segmenter) *Engine {
	if engine.initialized {
		log.Fatal(`Do not re-initialize the engine, 
			WithGse should call before initialize the engine.`)
	}

	engine.segmenter = segmenter
	engine.loaded = true
	return engine
}

func (engine *Engine) initDef(options types.EngineOpts) types.EngineOpts {
	if options.GseDict == "" && !options.NotUseGse && !engine.loaded {
		log.Printf("Dictionary file path is empty, load the default dictionary file.")
		options.GseDict = "zh"
	}

	if options.UseStore == true && options.StoreFolder == "" {
		log.Printf("Store file path is empty, use default folder path.")
		options.StoreFolder = DefaultPath
		// os.MkdirAll(DefaultPath, 0777)
	}

	return options
}

// Init initialize the engine
func (engine *Engine) Init(options types.EngineOpts) {
	// 将线程数设置为CPU数
	// runtime.GOMAXPROCS(runtime.NumCPU())
	// runtime.GOMAXPROCS(128)

	// 初始化初始参数
	if engine.initialized {
		log.Fatal("Do not re-initialize the engine.")
	}
	options = engine.initDef(options)

	options.Init()
	engine.initOptions = options
	engine.initialized = true

	if !options.NotUseGse {
		if !engine.loaded {
			// 载入分词器词典
			engine.segmenter.LoadDict(options.GseDict)
			engine.loaded = true
		}

		// 初始化停用词
		engine.stopTokens.Init(options.StopTokenFile)
	}

	// 初始化索引器和排序器
	// 启动每个索引器内部的正向索引恢复协程和反向索引恢复协程，启动每个排序器内部索引恢复协程，并阻塞等待所有协助全部执行完毕
	wg:=sync.WaitGroup{}
	wg.Add(options.NumShards*2)
	for shard := 0; shard < options.NumShards; shard++ {
		engine.rankers = append(engine.rankers, &core.Ranker{})
		engine.rankers[shard].Init(shard,options.IDOnly)
		engine.indexers = append(engine.indexers, &core.Indexer{})
		engine.indexers[shard].Init(shard,options.StoreIndexBufLen, *options.IndexerOpts,engine.rankers[shard])
		dbPathForwardIndex := engine.initOptions.StoreFolder + "/" +
			StoreFilePrefix + ".forwardindex." + strconv.Itoa(shard)
		dbPathReverseIndex := engine.initOptions.StoreFolder + "/" +
			StoreFilePrefix + ".reversedindex." + strconv.Itoa(shard)
		go engine.indexers[shard].StoreRecoverForwardIndex(dbPathForwardIndex,engine.initOptions.StoreEngine,&wg)
		go engine.indexers[shard].StoreRecoverReverseIndex(dbPathReverseIndex,engine.initOptions.StoreEngine,&wg)
	}
	wg.Wait()
	log.Println("index recover finish")
	for _, indexer := range engine.indexers {
		engine.numDocsIndexed+=indexer.GetNumDocs()
		engine.numTokenIndexAdded+=indexer.GetNumTotalTokenLen()
		engine.numDocsStored+=indexer.GetNumDocsStore()
	}
	// 初始化分词器通道
	engine.segmenterChan = make(
		chan segmenterReq, options.NumGseThreads)

	// 初始化索引器通道
	engine.Indexer(options)

	// 初始化排序器通道
	engine.Ranker(options)

	// engine.CheckMem(engine.initOptions.UseStore)
	engine.CheckMem()


	// 启动分词器
	for iThread := 0; iThread < options.NumGseThreads; iThread++ {
		go engine.segmenterWorker()
	}

	// 启动索引器和排序器以及各自对应的持久化工作协程
	for shard := 0; shard < options.NumShards; shard++ {
		go engine.indexerAddDoc(shard)
		go engine.indexerRemoveDoc(shard)
		go engine.indexers[shard].StoreUpdateForWardIndexWorker()
		go engine.indexers[shard].StoreUpdateReverseIndexWorker()

		for i := 0; i < options.NumIndexerThreads; i++ {
			go engine.indexerLookup(shard)
		}
		for i := 0; i < options.NumRankerThreads; i++ {
			go engine.rankerRank(shard)
		}
	}
}

// IndexDoc add the document to the index
// 将文档加入索引
//
// 输入参数：
//  docId	      标识文档编号，必须唯一，docId == 0 表示非法文档（用于强制刷新索引），[1, +oo) 表示合法文档
//  data	      见 DocIndexData 注释
//  forceUpdate 是否强制刷新 cache，如果设为 true，则尽快添加到索引，否则等待 cache 满之后一次全量添加
//
// 注意：
//      1. 这个函数是线程安全的，请尽可能并发调用以提高索引速度
//      2. 这个函数调用是非同步的，也就是说在函数返回时有可能文档还没有加入索引中，因此
//         如果立刻调用Search可能无法查询到这个文档。强制刷新索引请调用FlushIndex函数。
func (engine *Engine) IndexDoc(docId string, data types.DocData,
	forceUpdate ...bool) {
	engine.Index(docId, data, forceUpdate...)
}

// Index add the document to the index
func (engine *Engine) Index(docId string, data types.DocData,
	forceUpdate ...bool) {

	var force bool
	if len(forceUpdate) > 0 {
		force = forceUpdate[0]
	}

	// data.Tokens
	engine.internalIndexDoc(docId, data, force)
}

func (engine *Engine) internalIndexDoc(docId string, data types.DocData,
	forceUpdate bool) {

	if !engine.initialized {
		log.Fatal("The engine must be initialized first.")
	}

	hash := murmur.Sum32(fmt.Sprintf("%s%s", docId, data.Content))
	engine.segmenterChan <- segmenterReq{
		docId: docId, hash: hash, data: data, forceUpdate: forceUpdate}
}

// RemoveDoc remove the document from the index
// 将文档从索引中删除
//
// 输入参数：
//  docId	      标识文档编号，必须唯一，docId == 0 表示非法文档（用于强制刷新索引），[1, +oo) 表示合法文档
//  forceUpdate 是否强制刷新 cache，如果设为 true，则尽快删除索引，否则等待 cache 满之后一次全量删除
//
// 注意：
//      1. 这个函数是线程安全的，请尽可能并发调用以提高索引速度
//      2. 这个函数调用是非同步的，也就是说在函数返回时有可能文档还没有加入索引中，因此
//         如果立刻调用 Search 可能无法查询到这个文档。强制刷新索引请调用 FlushIndex 函数。
func (engine *Engine) RemoveDoc(docId string, forceUpdate ...bool) {
	var force bool
	if len(forceUpdate) > 0 {
		force = forceUpdate[0]
	}

	if !engine.initialized {
		log.Fatal("The engine must be initialized first.")
	}

	for shard := 0; shard < engine.initOptions.NumShards; shard++ {
		engine.indexerRemoveDocChans[shard] <- indexerRemoveDocReq{
			docId: docId, forceUpdate: force}
	}
}

// // 获取文本的分词结果
// func (engine *Engine) Tokens(text []byte) (tokens []string) {
// 	querySegments := engine.segmenter.Segment(text)
// 	for _, s := range querySegments {
// 		token := s.Token().Text()
// 		if !engine.stopTokens.IsStopToken(token) {
// 			tokens = append(tokens, token)
// 		}
// 	}
// 	return tokens
// }

// Segment get the word segmentation result of the text
// 获取文本的分词结果, 只分词与过滤弃用词
func (engine *Engine) Segment(content string) (keywords []string) {

	var segments []string
	hmm := engine.initOptions.Hmm

	if engine.initOptions.GseMode {
		segments = engine.segmenter.CutSearch(content, hmm)
	} else {
		segments = engine.segmenter.Cut(content, hmm)
	}

	for _, token := range segments {
		if !engine.stopTokens.IsStopToken(token) {
			keywords = append(keywords, token)
		}
	}

	return
}

// Tokens get the engine tokens
func (engine *Engine) Tokens(request types.SearchReq) (tokens []string) {
	// 收集关键词
	// tokens := []string{}
	if request.Text != "" {
		reqText := strings.ToLower(request.Text)
		if engine.initOptions.NotUseGse {
			tokens = strings.Split(reqText, " ")
		} else {
			// querySegments := engine.segmenter.Segment([]byte(reqText))
			// tokens = engine.Tokens([]byte(reqText))
			tokens = engine.Segment(reqText)
		}

		// 叠加 tokens
		for _, t := range request.Tokens {
			tokens = append(tokens, t)
		}

		return
	}

	for _, t := range request.Tokens {
		tokens = append(tokens, t)
	}
	return
}

func maxRankOutput(rankOpts types.RankOpts, rankLen int) (int, int) {
	var start, end int
	if rankOpts.MaxOutputs == 0 {
		start = utils.MinInt(rankOpts.OutputOffset, rankLen)
		end = rankLen
		return start, end
	}

	start = utils.MinInt(rankOpts.OutputOffset, rankLen)
	end = utils.MinInt(start+rankOpts.MaxOutputs, rankLen)
	return start, end
}

func (engine *Engine) rankOutID(rankerOutput rankerReturnReq,
	rankOutArr types.ScoredIDs) types.ScoredIDs {
	for _, doc := range rankerOutput.docs.(types.ScoredIDs) {
		rankOutArr = append(rankOutArr, doc)
	}
	return rankOutArr
}

func (engine *Engine) rankOutDocs(rankerOutput rankerReturnReq,
	rankOutArr types.ScoredDocs) types.ScoredDocs {
	for _, doc := range rankerOutput.docs.(types.ScoredDocs) {
		rankOutArr = append(rankOutArr, doc)
	}
	return rankOutArr
}

// NotTimeOut not set engine timeout
func (engine *Engine) NotTimeOut(request types.SearchReq,
	rankerReturnChan chan rankerReturnReq) (
	rankOutArr interface{}, numDocs int) {

	var (
		rankOutID  types.ScoredIDs
		rankOutDoc types.ScoredDocs
		idOnly     = engine.initOptions.IDOnly
	)

	for shard := 0; shard < engine.initOptions.NumShards; shard++ {
		rankerOutput := <-rankerReturnChan
		if !request.CountDocsOnly {
			if rankerOutput.docs != nil {
				if idOnly {
					rankOutID = engine.rankOutID(rankerOutput, rankOutID)
				} else {
					rankOutDoc = engine.rankOutDocs(rankerOutput, rankOutDoc)
				}
			}
		}
		numDocs += rankerOutput.numDocs
	}

	if idOnly {
		rankOutArr = rankOutID
		return
	}

	rankOutArr = rankOutDoc
	return
}

// TimeOut set engine timeout
func (engine *Engine) TimeOut(request types.SearchReq,
	rankerReturnChan chan rankerReturnReq) (
	rankOutArr interface{}, numDocs int, isTimeout bool) {

	deadline := time.Now().Add(time.Nanosecond *
		time.Duration(NumNanosecondsInAMillisecond*request.Timeout))

	var (
		rankOutID  types.ScoredIDs
		rankOutDoc types.ScoredDocs
		idOnly     = engine.initOptions.IDOnly
	)

	for shard := 0; shard < engine.initOptions.NumShards; shard++ {
		select {
		case rankerOutput := <-rankerReturnChan:
			if !request.CountDocsOnly {
				if rankerOutput.docs != nil {
					if idOnly {
						rankOutID = engine.rankOutID(rankerOutput, rankOutID)
					} else {
						rankOutDoc = engine.rankOutDocs(rankerOutput, rankOutDoc)
					}
				}
			}
			numDocs += rankerOutput.numDocs
		case <-time.After(deadline.Sub(time.Now())):
			isTimeout = true
			break
		}
	}

	if idOnly {
		rankOutArr = rankOutID
		return
	}

	rankOutArr = rankOutDoc
	return
}

// RankID rank docs by types.ScoredIDs
func (engine *Engine) RankID(request types.SearchReq, rankOpts types.RankOpts,
	tokens []string, rankerReturnChan chan rankerReturnReq) (output types.SearchResp) {
	// 从通信通道读取排序器的输出
	numDocs := 0
	rankOutput := types.ScoredIDs{}

	//**********/ begin
	timeout := request.Timeout
	isTimeout := false
	if timeout <= 0 {
		// 不设置超时
		rankOutArr, num := engine.NotTimeOut(request, rankerReturnChan)
		rankOutput = rankOutArr.(types.ScoredIDs)
		numDocs += num
	} else {
		// 设置超时
		rankOutArr, num, timeout := engine.TimeOut(request, rankerReturnChan)
		rankOutput = rankOutArr.(types.ScoredIDs)
		numDocs += num
		isTimeout = timeout
	}

	// 再排序
	if !request.CountDocsOnly && !request.Orderless {
		if rankOpts.ReverseOrder {
			sort.Sort(sort.Reverse(rankOutput))
		} else {
			sort.Sort(rankOutput)
		}
	}

	// 准备输出
	output.Tokens = tokens
	// 仅当 CountDocsOnly 为 false 时才充填 output.Docs
	if !request.CountDocsOnly {
		if request.Orderless {
			// 无序状态无需对 Offset 截断
			output.Docs = rankOutput
		} else {
			rankOutLen := len(rankOutput)
			start, end := maxRankOutput(rankOpts, rankOutLen)

			output.Docs = rankOutput[start:end]
		}
	}

	output.NumDocs = numDocs
	output.Timeout = isTimeout

	return
}

// Ranks rank docs by types.ScoredDocs
func (engine *Engine) Ranks(request types.SearchReq, rankOpts types.RankOpts,
	tokens []string, rankerReturnChan chan rankerReturnReq) (output types.SearchResp) {
	// 从通信通道读取排序器的输出
	numDocs := 0
	rankOutput := types.ScoredDocs{}

	//**********/ begin
	timeout := request.Timeout
	isTimeout := false
	if timeout <= 0 {
		// 不设置超时
		rankOutArr, num := engine.NotTimeOut(request, rankerReturnChan)
		rankOutput = rankOutArr.(types.ScoredDocs)
		numDocs += num
	} else {
		// 设置超时
		rankOutArr, num, timeout := engine.TimeOut(request, rankerReturnChan)
		rankOutput = rankOutArr.(types.ScoredDocs)
		numDocs += num
		isTimeout = timeout
	}

	// 再排序
	if !request.CountDocsOnly && !request.Orderless {
		if rankOpts.ReverseOrder {
			sort.Sort(sort.Reverse(rankOutput))
		} else {
			sort.Sort(rankOutput)
		}
	}

	// 准备输出
	output.Tokens = tokens
	// 仅当 CountDocsOnly 为 false 时才充填 output.Docs
	if !request.CountDocsOnly {
		if request.Orderless {
			// 无序状态无需对 Offset 截断
			output.Docs = rankOutput
		} else {
			rankOutLen := len(rankOutput)
			start, end := maxRankOutput(rankOpts, rankOutLen)

			output.Docs = rankOutput[start:end]
		}
	}

	output.NumDocs = numDocs
	output.Timeout = isTimeout

	return
}

// SearchDoc find the document that satisfies the search criteria.
// This function is thread safe, return not IDonly
func (engine *Engine) SearchDoc(request types.SearchReq) (output types.SearchDoc) {
	resp := engine.Search(request)
	return types.SearchDoc{
		BaseResp: resp.BaseResp,
		Docs:     resp.Docs.(types.ScoredDocs),
	}
}

// SearchID find the document that satisfies the search criteria.
// This function is thread safe, return IDonly
func (engine *Engine) SearchID(request types.SearchReq) (output types.SearchID) {
	// return types.SearchID(engine.Search(request))
	resp := engine.Search(request)
	return types.SearchID{
		BaseResp: resp.BaseResp,
		Docs:     resp.Docs.(types.ScoredIDs),
	}
}

// Search find the document that satisfies the search criteria.
// This function is thread safe
// 查找满足搜索条件的文档，此函数线程安全
func (engine *Engine) Search(request types.SearchReq) (output types.SearchResp) {
	if !engine.initialized {
		log.Fatal("The engine must be initialized first.")
	}

	tokens := engine.Tokens(request)

	var rankOpts types.RankOpts
	if request.RankOpts == nil {
		rankOpts = *engine.initOptions.DefRankOpts
	} else {
		rankOpts = *request.RankOpts
	}

	if rankOpts.ScoringCriteria == nil {
		rankOpts.ScoringCriteria = engine.initOptions.DefRankOpts.ScoringCriteria
	}

	// 建立排序器返回的通信通道
	rankerReturnChan := make(
		chan rankerReturnReq, engine.initOptions.NumShards)

	// 生成查找请求
	lookupRequest := indexerLookupReq{
		countDocsOnly:    request.CountDocsOnly,
		tokens:           tokens,
		labels:           request.Labels,
		docIds:           request.DocIds,
		options:          rankOpts,
		rankerReturnChan: rankerReturnChan,
		orderless:        request.Orderless,
		logic:            request.Logic,
	}

	// 向索引器发送查找请求
	for shard := 0; shard < engine.initOptions.NumShards; shard++ {
		engine.indexerLookupChans[shard] <- lookupRequest
	}

	if engine.initOptions.IDOnly {
		output = engine.RankID(request, rankOpts, tokens, rankerReturnChan)
		return
	}

	output = engine.Ranks(request, rankOpts, tokens, rankerReturnChan)
	return
}

// Flush block wait until all indexes are added
// 阻塞等待直到所有索引添加完毕
func (engine *Engine) Flush() {
	wg :=sync.WaitGroup{}
	wg.Add(engine.initOptions.NumShards)
	for shard := 0; shard < engine.initOptions.NumShards; shard++ {
		go engine.indexers[shard].Flush(&wg)
	}
	wg.Wait()
}

// FlushIndex block wait until all indexes are added
// 阻塞等待直到所有索引添加完毕
func (engine *Engine) FlushIndex() {
	engine.Flush()
}

// Close close the engine
// 关闭引擎
func (engine *Engine) Close() {
	engine.Flush()
	if engine.initOptions.UseStore {
		for _, indexer := range engine.indexers {
			dbf:=indexer.GetForwardIndexDB()
			if dbf!=nil {
				dbf.Close()
			}
			dbr:=indexer.GetReverseIndexDB()
			if dbr!=nil {
				dbr.Close()
			}
		}
	}
}

// 从文本hash得到要分配到的 shard
func (engine *Engine) getShard(hash uint32) int {
	return int(hash - hash/uint32(engine.initOptions.NumShards)*
		uint32(engine.initOptions.NumShards))
}
