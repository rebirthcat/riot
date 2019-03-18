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

package types

import "sync"

// BaseResp search response options
type BaseResp struct {
	// 搜索用到的关键词
	Tokens []string

	// 类别
	// Class string

	// 搜索到的文档，已排序
	// Docs []ScoredDoc
	// Docs interface{}

	// 搜索是否超时。超时的情况下也可能会返回部分结果
	Timeout bool

	// 搜索到的文档个数。注意这是全部文档中满足条件的个数，可能比返回的文档数要大
	NumDocs int
}

// SearchResp search response options
type SearchResp struct {
	BaseResp
	// 搜索到的文档，已排序
	Docs interface{}
}

// SearchDoc search response options
//type SearchDoc struct {
//	BaseResp
//	// 搜索到的文档，已排序
//	Docs []ScoredDoc
//}

// SearchID search response options
type SearchID struct {
	BaseResp
	// 搜索到的文档，已排序
	Docs []ScoredID
}

// Content search content
//type Content struct {
//	// new Content
//	Content string
//
//	// new 属性 Attri
//	Attri interface{}
//
//	// new 返回评分字段
//	Fields interface{}
//}

// ScoredDoc scored the document
//type ScoredDoc struct {
//	ScoredID
//
//	// new 返回文档 Content
//	Content string
//	// new 返回文档属性 Attri
//	Attri interface{}
//	// new 返回评分字段
//	Fields interface{}
//}
//
//// ScoredDocs 为了方便排序
//type ScoredDocs []ScoredDoc
//
//func (docs ScoredDocs) Len() int {
//	return len(docs)
//}
//
//func (docs ScoredDocs) Swap(i, j int) {
//	docs[i], docs[j] = docs[j], docs[i]
//}
//
//func (docs ScoredDocs) Less(i, j int) bool {
//	// 为了从大到小排序，这实际上实现的是 More 的功能
//	min := utils.MinInt(len(docs[i].Scores), len(docs[j].Scores))
//	for iScore := 0; iScore < min; iScore++ {
//		if docs[i].Scores[iScore] > docs[j].Scores[iScore] {
//			return true
//		} else if docs[i].Scores[iScore] < docs[j].Scores[iScore] {
//			return false
//		}
//	}
//	return len(docs[i].Scores) > len(docs[j].Scores)
//}

/*
  ______   .__   __.  __      ____    ____  __   _______
 /  __  \  |  \ |  | |  |     \   \  /   / |  | |       \
|  |  |  | |   \|  | |  |      \   \/   /  |  | |  .--.  |
|  |  |  | |  . `  | |  |       \_    _/   |  | |  |  |  |
|  `--'  | |  |\   | |  `----.    |  |     |  | |  '--'  |
 \______/  |__| \__| |_______|    |__|     |__| |_______/

*/

// ScoredID scored doc only id
type ScoredID struct {
	DocId string

	//BM25 float32
	// 文档的打分值
	Scores float32

	// TokenProximity 关键词在文档中的紧邻距离，
	// 紧邻距离的含义见 computeTokenProximity 的注释。
	// 仅当索引类型为 LocsIndex 时返回有效值。
	TokenProximity int32


	// 用于生成摘要的关键词在文本中的字节位置，
	// 该切片长度和 SearchResp.Tokens 的长度一样
	// 只有当 IndexType == LocsIndex 时不为空
	TokenSnippetLocs []int

	// 关键词出现的位置
	// 只有当 IndexType == LocsIndex 时不为空
	TokenLocs [][]int
}

// ScoredIDs 为了方便排序
type ScoredIDs []*ScoredID

func (docs ScoredIDs) Len() int {
	return len(docs)
}

func (docs ScoredIDs) Swap(i, j int) {
	docs[i], docs[j] = docs[j], docs[i]
}

func (docs ScoredIDs) Less(i, j int) bool {

	return docs[i].Scores > docs[j].Scores
}

type HeapNode struct {
	ScoreObj *ScoredID
	ShareNum int
	IndexPointer int
}

type NodeHeap []HeapNode

func (h NodeHeap) Len() int {
	return len(h)
}

func (h NodeHeap) Swap(i, j int) {
	h[i],h[j]=h[j],h[i]
}

func (h NodeHeap)Less(i,j int) bool {
	return h[i].ScoreObj.Scores>h[j].ScoreObj.Scores
}

func (h *NodeHeap) Push(x interface{}) {
	*h=append(*h,x.(HeapNode))
}

func (h *NodeHeap)Pop()interface{}  {
	old:=*h
	n:=len(old)
	if n == 0 {
		return nil
	}
	x:=old[n-1]
	*h=old[0:n-1]
	return x
}

type OutputPage struct {
	PageSize int
	PageNum   int
}




var ScoreIDPool=&sync.Pool{
	New: func() interface{} {
		return new(ScoredID)
	},
}


