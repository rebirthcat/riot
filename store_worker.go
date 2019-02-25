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
	"riot/core"

	"encoding/gob"
)

type storeRevertIndexReq struct {
	token string
	keywordIndices core.KeywordIndices
	docTokenLen float32
}

type storeRankerIndexReq struct {

	fieldKey string
	fieldValue interface{}

	docKey string
	docValue bool

	contentKey string
	contentValue string

	attriKey string
	attriValue interface{}
}

type storeForwardIndex struct {
	//用于恢复docTokenLens map[string]float32
	tokenLen float32
	//用于恢复docsState map[string]int
	field interface{}
}

func (engine *Engine)storeRecoverForwards(shard int)  {

	numDocs:= uint64(0)
	numDocsBytes,err:=engine.dbForwards[shard].Get([]byte("numDocs"))
	if err != nil {
		//log
		numDocs=0
	}
	bufnumDocs := bytes.NewReader(numDocsBytes)
	dec:=gob.NewDecoder(bufnumDocs)
	err=dec.Decode(&numDocs)
	if err!=nil {
		//log
	}
	totalTokenLen:=float32(0)
	totalTokenLenBytes,err:=engine.dbForwards[shard].Get([]byte("totalTokenLen"))
	if err != nil {
		//log
	}
	bufTotalTokenLen:=bytes.NewBuffer(totalTokenLenBytes)
	dec=gob.NewDecoder(bufTotalTokenLen)
	err=dec.Decode(&totalTokenLen)
	if err != nil {
		//log
	}
	docsState:=make(map[string]int,numDocs)
	fields:=make(map[string]interface{},numDocs)
	docsExist:=make(map[string]bool,numDocs)
	docTokenLens:=make(map[string]float32,numDocs)

	engine.dbReverses[shard].ForEach(func(k, v []byte) error {
		key, value := k, v
		// 得到docID
		keystring := string(key)
		if keystring!="numDocs"&&keystring!="totalTokenLen"{
			buf := bytes.NewReader(value)
			dec := gob.NewDecoder(buf)
			//var data types.DocData
			var fowardindex storeForwardIndex
			err := dec.Decode(&fowardindex)
			if err == nil {
				// 添加索引
				docsState[keystring]=0
				fields[keystring]=fowardindex.field
				docsExist[keystring]=true
				docTokenLens[keystring]=fowardindex.tokenLen
			}
		}
		return nil
	})
	//恢复indexer
	engine.indexers[shard].SetdocTokenLens(docTokenLens)
	engine.indexers[shard].SetTotalTokenLen(totalTokenLen)
	engine.indexers[shard].SetNumDocs(numDocs)
	engine.indexers[shard].SetDocsState(docsState)
	//恢复ranker
	engine.rankers[shard].SetFields(fields)
	engine.rankers[shard].SetDocs(docsExist)
	engine.storeIndexRecoverChan <- true
}





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
	engine.storeIndexRecoverChan <- true
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
