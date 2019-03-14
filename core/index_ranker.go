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

package core

//import (
//	"log"
//	"github.com/rebirthcat/riot/types"
//	"github.com/rebirthcat/riot/utils"
//	"sort"
//)
//
//
//func maxOutput(options types.RankOpts, docsLen int) (int, int) {
//	var start, end int
//	if options.MaxOutputs != 0 {
//		start = utils.MinInt(options.OutputOffset, docsLen)
//		end = utils.MinInt(options.OutputOffset+options.MaxOutputs, docsLen)
//		return start, end
//	}
//
//	start = utils.MinInt(options.OutputOffset, docsLen)
//	end = docsLen
//	return start, end
//}
////d
//func (indexer *Indexer) rankOutIDs(docs types.ScoredIDs, options types.RankOpts,
//	countDocsOnly bool) (outputDocs types.ScoredIDs, numDocs int) {
//	for _, d := range docs {
//		indexer.rankerLock.RLock()
//		// 判断 doc 是否存在
//		if fs, ok := indexer.rankerLock.fields[d.DocId]; ok {
//
//			//fs := indexer.rankerLock.fields[d.DocId]
//			indexer.rankerLock.RUnlock()
//
//			// 计算评分并剔除没有分值的文档
//			scores := options.ScoringCriteria.Score(d, fs)
//			if len(scores) > 0 {
//				if !countDocsOnly {
//					outputDocs = append(outputDocs,
//						types.ScoredID{
//							DocId:            d.DocId,
//							Scores:           scores,
//							TokenSnippetLocs: d.TokenSnippetLocs,
//							TokenLocs:        d.TokenLocs,
//						})
//				}
//				numDocs++
//			}
//		} else {
//			indexer.rankerLock.RUnlock()
//		}
//	}
//
//	return
//}
//
//// RankDocID rank docs by types.ScoredIDs
//func (indexer *Indexer) RankDocID(docs []types.IndexedDoc,
//	options types.RankOpts, countDocsOnly bool) (types.ScoredIDs, int) {
//
//	outputDocs, numDocs := indexer.rankOutIDs(docs, options, countDocsOnly)
//
//	// 排序
//	if !countDocsOnly {
//		if options.ReverseOrder {
//			sort.Sort(sort.Reverse(outputDocs))
//		} else {
//			sort.Sort(outputDocs)
//		}
//		// 当用户要求只返回部分结果时返回部分结果
//		docsLen := len(outputDocs)
//		start, end := maxOutput(options, docsLen)
//
//		return outputDocs[start:end], numDocs
//	}
//
//	return outputDocs, numDocs
//}
//
//
//// Rank rank docs
//// 给文档评分并排序
//func (indexer *Indexer) Rank(docs []types.IndexedDoc,
//	options types.RankOpts, countDocsOnly bool) (interface{}, int) {
//
//	if indexer.initialized == false {
//		log.Fatal("The Indexer has not been initialized.")
//	}
//
//	// 对每个文档评分
//	outputDocs, numDocs := indexer.RankDocID(docs, options, countDocsOnly)
//	return outputDocs, numDocs
//}
