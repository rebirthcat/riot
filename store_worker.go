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

package riot

import (
	"bytes"

	"github.com/rebirthcat/riot/core"

	"encoding/gob"
)


//静态恢复
type storeForwardIndex struct {
	//用于恢复docTokenLens map[string]float32
	tokenLen float32
}

type storeRankerIndex struct {
	docExist bool
	field interface{}
}

//动态请求
type storeForwardIndexReq struct {

}

type storeRevertIndexReq struct {
	token string
	keywordIndices core.KeywordIndices
}

type storeRankerIndexReq struct {
	docID string
	docExist bool
	field interface{}
}


//恢复正向索引，所谓正向索引就是值key是docID的map数据，内容包括如下
//numDocs  		总文档数  	对应于indexer.numDocs字段
//totalTokenLen 总关键词数（有重复计数）对应于indexer.totalTokenLen字段
//docsState		每个文档的状态   对应于indexer.tableLock.docsState字段
//docTokenLens  每个文档的关键词数量 对应于index.docTokenLens


func (engine *Engine)storeRecoverForwards(shard int)  {
	//indexer中的字段
	numDocs:= uint64(0)
	totalTokenLen:=float32(0)
	docsState:=make(map[string]int,numDocs)
	docTokenLens:=make(map[string]float32,numDocs)
	//ranker中的字段
	//fields:=make(map[string]interface{},numDocs)
	//docsExist:=make(map[string]bool,numDocs)

	engine.dbForwards[shard].ForEach(func(k, v []byte) error {
		key, value := k, v
		// 得到docID
		keystring := string(key)
		buf := bytes.NewReader(value)
		dec := gob.NewDecoder(buf)
		//正向索引结构
		var fowardindex storeForwardIndex
		err := dec.Decode(&fowardindex)
		if err == nil {
			docsState[keystring] = 0
			docTokenLens[keystring] = fowardindex.tokenLen
			numDocs++
			totalTokenLen += fowardindex.tokenLen
		}
		return nil
	})
	//恢复indexer
	engine.indexers[shard].SetTotalTokenLen(totalTokenLen)
	engine.indexers[shard].SetNumDocs(numDocs)
	engine.indexers[shard].SetdocTokenLens(docTokenLens)
	engine.indexers[shard].SetDocsState(docsState)
	engine.storeRecoverForwardIndexChan <- true
}

//恢复反向索引，即倒排索引，也就是key是关键词，value为id数组的map数据，对应于indexer.TableLock.table字段，是倒排索引最核心的数据
func (engine *Engine)storeRecoverReverses(shard int)  {
	table:=make(map[string]*core.KeywordIndices)
	engine.dbReverses[shard].ForEach(func(k, v []byte) error {
		key, value := k, v
		// 得到docID
		keystring := string(key)
		buf := bytes.NewReader(value)
		dec := gob.NewDecoder(buf)
		//var data types.DocData
		var keywordIndices core.KeywordIndices
		err := dec.Decode(&keywordIndices)
		if err == nil {
			// 添加索引
			table[keystring]=&keywordIndices
		}
		return nil
	})
	engine.indexers[shard].SetTable(table)
	engine.storeRecoverReverseIndexChan <- true
}

//恢复ranker数据
//fields		每个文档的排序字段 对应于ranker.lock.fields字段
//docsExist     每个文档是正在索引中 对应于ranker.lock.docsExist字段

func (engine *Engine)storeRecoverRankers(shard int)  {
	//ranker中的字段
	fields:=make(map[string]interface{})
	docsExist:=make(map[string]bool)
	engine.dbReverses[shard].ForEach(func(k, v []byte) error {
		key, value := k, v
		// 得到docID
		keystring := string(key)
		buf := bytes.NewReader(value)
		dec := gob.NewDecoder(buf)
		//var data types.DocData
		var rankeindex storeRankerIndex
		err := dec.Decode(&rankeindex)
		if err == nil {
			// 添加索引
			docsExist[keystring]=rankeindex.docExist
			fields[keystring]=rankeindex.field
		}
		return nil
	})
	engine.rankers[shard].SetDocs(docsExist)
	engine.rankers[shard].SetFields(fields)
	engine.storeRecoverRankerIndexChan <- true
}



//func (engine *Engine)storeRevertIndexAdd(shard int)  {
//	for {
//		request:=<-engine.storeRevertIndexChans[shard]
//		b:=[]byte(request.token)
//
//		var buf bytes.Buffer
//		enc:=gob.NewEncoder(&buf)
//		err:=enc.Encode(request)
//		if err != nil {
//			//log
//			continue
//		}
//		engine.dbs[shard].Set(b,buf.Bytes())
//	}
//}
//
//
//
//
//type storeIndexDocReq struct {
//	docId string
//	data  types.DocData
//	// data        types.DocumentIndexData
//}
//
//func (engine *Engine) storeIndexDoc(shard int) {
//	for {
//		request := <-engine.storeIndexDocChans[shard]
//
//		// 得到 key
//		b := []byte(request.docId)
//
//		// 得到 value
//		var buf bytes.Buffer
//		enc := gob.NewEncoder(&buf)
//		err := enc.Encode(request.data)
//		if err != nil {
//			atomic.AddUint64(&engine.numDocsStored, 1)
//			continue
//		}
//
//		// has, err := engine.dbs[shard].Has(b[0:length])
//		// if err != nil {
//		// 	log.Println("engine.dbs[shard].Has(b[0:length]) ", err)
//		// }
//
//		// if has {
//		// 	engine.dbs[shard].Delete(b[0:length])
//		// }
//
//		// 将 key-value 写入数据库
//		engine.dbs[shard].Set(b, buf.Bytes())
//
//		atomic.AddUint64(&engine.numDocsStored, 1)
//	}
//}
//
//func (engine *Engine) storeRemoveDoc(docId string, shard uint32) {
//	// 得到 key
//	b := []byte(docId)
//	// 从数据库删除该key
//	engine.dbs[shard].Delete(b)
//}
//
//// storeInit persistent storage init worker
//func (engine *Engine) storeInit(shard int) {
//	engine.dbs[shard].ForEach(func(k, v []byte) error {
//		key, value := k, v
//		// 得到docID
//		docId := string(key)
//
//		// 得到 data
//		buf := bytes.NewReader(value)
//		dec := gob.NewDecoder(buf)
//		var data types.DocData
//		err := dec.Decode(&data)
//		if err == nil {
//			// 添加索引
//			engine.internalIndexDoc(docId, data, false)
//		}
//		return nil
//	})
//	engine.storeInitChan <- true
//}
