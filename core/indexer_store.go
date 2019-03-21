package core

import (
	"bytes"
	"encoding/gob"
	"github.com/rebirthcat/riot/store"
	"github.com/rebirthcat/riot/types"
	"log"
	"sync"
)



//系统重启时从持久化文件中读出来的一行数据所反序列化得到的类型
//type StoreForwardIndex struct {
//	//用于恢复docTokenLens map[string]float32
//	TokenLen float32
//	//用于恢复ranker的排序字段field
//	Field interface{}
//	FieldFilter interface{}
//}

//type StoreForwardIndexReq struct {
//	DocID string
//	Remove bool
//	DocTokenLen float32
//	Field interface{}
//	FieldFilter interface{}
//
//}

type StoreReverseIndex struct {
	DocIds []string
	Frequencies []float32
	Locations   [][]int
}

//在线持久化请求结构
type StoreReverseIndexReq struct {
	Token string
	KeywordIndices StoreReverseIndex
}


func (indexer *Indexer) GetForwardIndexDB() store.Store {
	return indexer.dbforwardIndex
}

func (indexer *Indexer)GetReverseIndexDB()store.Store  {
	return indexer.dbRevertIndex
}

func (indexer *Indexer) OpenForwardIndexDB(dbPath string,StoreEngine string)  {
	var erropen error
	indexer.dbforwardIndex, erropen= store.OpenStore(dbPath, StoreEngine)
	if indexer.dbforwardIndex == nil || erropen != nil {
		log.Fatal("Unable to open database ", dbPath, ": ", erropen)
	}
}

func (indexer *Indexer)OpenReverseIndexDB(dbPath string,StoreEngine string)  {
	var erropen error
	indexer.dbRevertIndex,erropen=store.OpenStore(dbPath,StoreEngine)
	if indexer.dbRevertIndex==nil||erropen!=nil {
		log.Fatal("Unable to open database ", dbPath, ": ", erropen)
	}
}



func (indexer *Indexer)StoreRecoverReverseIndex(tokenNumber uint64, wg *sync.WaitGroup)  {
	table:=make(map[string]*KeywordIndices,tokenNumber)
	if indexer.dbRevertIndex==nil {
		log.Fatalf("indexer %v dbreverse is not open",indexer.shardNumber)
	}
	indexer.dbRevertIndex.ForEach(func(k, v []byte) error {
		key, value := k, v
		// 得到docID
		keystring := string(key)
		buf := bytes.NewReader(value)
		dec := gob.NewDecoder(buf)
		var storereverse StoreReverseIndex
		err := dec.Decode(&storereverse)
		if err == nil {
			// 添加索引
			if indexer.initOptions.IndexType!=types.DocIdsIndex {
				table[keystring]=&KeywordIndices{
					docIds:storereverse.DocIds,
					frequencies:storereverse.Frequencies,
					locations:storereverse.Locations,
				}
			}else {
				table[keystring]=&KeywordIndices{
					docIds:storereverse.DocIds,
					frequencies:nil,
					locations:nil,
				}
			}

		}
		return nil
	})
	indexer.tableLock.table=table
	log.Printf("indexer%v reverseindex recover finish",indexer.shardNumber)
	if wg!=nil {
		wg.Done()
	}

}

func (indexer *Indexer)StoreReverseIndexOneTime(wg *sync.WaitGroup)  {
	if indexer.dbRevertIndex==nil {
		log.Fatalf("indexer %v dbreverse is not open",indexer.shardNumber)
	}
	indexer.tableLock.RLock()
	for Token,KeywordIndices:=range indexer.tableLock.table{
		buf:=bytes.Buffer{}
		enc:=gob.NewEncoder(&buf)
		enc.Encode(StoreReverseIndex{
			DocIds:KeywordIndices.docIds,
			Frequencies:KeywordIndices.frequencies,
			Locations:KeywordIndices.locations,
		})
		indexer.dbRevertIndex.Set([]byte(Token),buf.Bytes())
	}
	indexer.tableLock.RUnlock()
	if wg!=nil {
		wg.Done()
	}
}

func (indexer *Indexer)StoreUpdateBegin()  {
	indexer.storeUpdateBegin=true
}


//系统正常运行中动态的添加索引的持久化

func (indexer *Indexer) StoreUpdateReverseIndexWorker() {
	if indexer.dbRevertIndex==nil {
		log.Fatalf("indexer %v dbreverse is not open",indexer.shardNumber)
	}
	for {
		request := <-indexer.storeUpdateReverseIndexChan
		buf:=bytes.Buffer{}
		enc:=gob.NewEncoder(&buf)
		enc.Encode(request.KeywordIndices)
		indexer.dbRevertIndex.Set([]byte(request.Token),buf.Bytes())
	}
}


//func (indexer *Indexer)StoreUpdateForWardIndexWorker()  {
//	if indexer.dbforwardIndex==nil {
//		log.Fatalf("indexer %v dbforward is not open",indexer.shardNumber)
//	}
//
//	for {
//	 	request := <-indexer.storeUpdateForwardIndexChan
//	 	//如果传过来的持久化请求中的DocTokenLen小于0,则是删除请求，即从RemoveDocs（）函数中传过来的
//	 	if request.Remove {
//	 		indexer.dbforwardIndex.Delete([]byte(request.DocID))
//	 		atomic.AddUint64(&indexer.numDocsStore,^uint64(1-1))
//	 		continue
//	 	}else {
//			buf := bytes.Buffer{}
//			enc := gob.NewEncoder(&buf)
//			if indexer.initOptions.IndexType!=types.DocIdsIndex {
//				enc.Encode(StoreForwardIndex{
//					TokenLen: request.DocTokenLen,
//					Field:    request.Field,
//					FieldFilter:request.FieldFilter,
//				})
//			}else {
//				enc.Encode(StoreForwardIndex{
//					Field:    request.Field,
//					FieldFilter:request.FieldFilter,
//				})
//			}
//			indexer.dbforwardIndex.Set([]byte(request.DocID), buf.Bytes())
//			atomic.AddUint64(&indexer.numDocsStore, 1)
//		}
//	}
//}

