package core

import (
	"bytes"
	"encoding/gob"
	"github.com/rebirthcat/riot/store"
	"log"
	"sync"
	"sync/atomic"
)

//系统重启时从持久化文件中读出来的一行数据所反序列化得到的类型
type StoreForwardIndex struct {
	//用于恢复docTokenLens map[string]float32
	tokenLen float32
	//用于恢复ranker的排序字段field
	field interface{}
}


//在线持久化请求结构
type StoreReverseIndexReq struct {
	Token string
	KeywordIndices KeywordIndices
}

type StoreForwardIndexReq struct {
	DocID string
	DocTokenLen float32
	Field interface{}
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



func (indexer *Indexer)StoreRecoverForwardIndex(dbPath string,StoreEngine string, wg *sync.WaitGroup)  {
	//indexer中的字段
	numDocs:= uint64(0)
	totalTokenLen:=float32(0)
	docsState:=make(map[string]int,numDocs)
	docTokenLens:=make(map[string]float32,numDocs)
	//ranker中的字段
	fields:=make(map[string]interface{},numDocs)
	docsExist:=make(map[string]bool,numDocs)
	var erropen error
	indexer.dbforwardIndex, erropen= store.OpenStore(dbPath, StoreEngine)
	if indexer.dbforwardIndex == nil || erropen != nil {
		log.Fatal("Unable to open database ", dbPath, ": ", erropen)
	}
	//defer indexer.dbforwardIndex.Close()
	indexer.dbforwardIndex.ForEach(func(k, v []byte) error {
		key, value := k, v
		// 得到docID
		keystring := string(key)
		buf := bytes.NewReader(value)
		dec := gob.NewDecoder(buf)
		//正向索引结构
		var fowardindex StoreForwardIndex
		err := dec.Decode(&fowardindex)
		log.Println(fowardindex)
		if err == nil {
			docsState[keystring] = 0
			docTokenLens[keystring] = fowardindex.tokenLen
			numDocs++
			totalTokenLen += fowardindex.tokenLen
			fields[keystring]=fowardindex.field
			docsExist[keystring]=true
		}
		return nil
	})
	//恢复indexer
	indexer.tableLock.Lock()
	indexer.tableLock.totalTokenLen=totalTokenLen
	indexer.tableLock.numDocs=numDocs
	indexer.tableLock.docTokenLens=docTokenLens
	indexer.tableLock.docsState=docsState
	indexer.tableLock.Unlock()
	atomic.AddUint64(&indexer.numDocsStore,numDocs)
	//恢复ranker
	indexer.ranker.lock.Lock()
	indexer.ranker.lock.fields=fields
	indexer.ranker.lock.docs=docsExist
	indexer.ranker.lock.Unlock()
	log.Printf("indexer%v forwardindex recover finish",indexer.shardNumber)
	wg.Done()
}



func (indexer *Indexer)StoreRecoverReverseIndex(dbPath string,StoreEngine string, wg *sync.WaitGroup)  {
	table:=make(map[string]*KeywordIndices)
	var erropen error
	indexer.dbRevertIndex,erropen=store.OpenStore(dbPath,StoreEngine)
	if indexer.dbRevertIndex==nil||erropen!=nil {
		log.Fatal("Unable to open database ", dbPath, ": ", erropen)
	}
	//defer indexer.dbRevertIndex.Close()
	indexer.dbRevertIndex.ForEach(func(k, v []byte) error {
		key, value := k, v
		// 得到docID
		keystring := string(key)
		buf := bytes.NewReader(value)
		dec := gob.NewDecoder(buf)
		//var data types.DocData
		var keywordIndices KeywordIndices
		err := dec.Decode(&keywordIndices)
		if err == nil {
			// 添加索引
			table[keystring]=&keywordIndices
		}
		return nil
	})
	indexer.tableLock.Lock()
	indexer.tableLock.table=table
	indexer.tableLock.Unlock()
	log.Printf("indexer%v reverseindex recover finish",indexer.shardNumber)
	wg.Done()
}



//func (indexer *Indexer)StoreUpdateForWardIndexWorker()  {
//	if indexer.dbforwardIndex==nil {
//		log.Fatalf("indexer %v dbforward is not open",indexer.shardNumber)
//	}
//	timer:=time.NewTimer(time.Millisecond*1000)
//	for {
//		timer.Reset(time.Millisecond*1000)
//		select {
//		case request := <-indexer.storeUpdateForwardIndexChan:
//			indexer.storeUpdateForwardIndexFinsh=false
//			if indexer.dbforwardIndex==nil {
//				log.Fatalf("indexer %shard_v  dbforwardIndex is not open,updatefailed",indexer.shardNumber)
//			}
//			//如果传过来的持久化请求中的DocTokenLen小于0,则是删除请求，即从RemoveDocs（）函数中传过来的
//			if request.DocTokenLen<0 {
//				indexer.dbforwardIndex.Delete([]byte(request.DocID))
//				atomic.AddUint64(&indexer.numDocsStore,^uint64(1-1))
//				continue
//			}else {
//				buf:=bytes.Buffer{}
//				enc:=gob.NewEncoder(&buf)
//				enc.Encode(StoreForwardIndex{
//					tokenLen:request.DocTokenLen,
//					field:request.Field,
//				})
//				indexer.dbforwardIndex.Set([]byte(request.DocID),buf.Bytes())
//				atomic.AddUint64(&indexer.numDocsStore,1)
//			}
//		case <-timer.C:
//			indexer.storeUpdateForwardIndexFinsh=true
//		}
//	}
//}
//
//func (indexer *Indexer) StoreUpdateReverseIndexWorker() {
//	if indexer.dbforwardIndex==nil {
//		log.Fatalf("indexer %v dbreverse is not open",indexer.shardNumber)
//	}
//	timer:=time.NewTimer(time.Millisecond*1000)
//	for {
//		select {
//		case request := <-indexer.storeUpdateReverseIndexChan:
//			indexer.storeUpdateReverseIndexFinsh=false
//			if indexer.dbRevertIndex==nil {
//				log.Fatalf("indexer shard_%v  dbRevertIndex is not open,updatefailed",indexer.shardNumber)
//			}
//			buf:=bytes.Buffer{}
//			enc:=gob.NewEncoder(&buf)
//			enc.Encode(request.KeywordIndices)
//			indexer.dbRevertIndex.Set([]byte(request.Token),buf.Bytes())
//		case <-timer.C:
//			indexer.storeUpdateReverseIndexFinsh=true
//		}
//	}
//}




func (indexer *Indexer)StoreUpdateForWardIndexWorker()  {
	if indexer.dbforwardIndex==nil {
		log.Fatalf("indexer %v dbforward is not open",indexer.shardNumber)
	}

	for {
	 	request := <-indexer.storeUpdateForwardIndexChan
	 	//如果传过来的持久化请求中的DocTokenLen小于0,则是删除请求，即从RemoveDocs（）函数中传过来的
	 	if request.DocTokenLen<0 {
	 		indexer.dbforwardIndex.Delete([]byte(request.DocID))
	 		atomic.AddUint64(&indexer.numDocsStore,^uint64(1-1))
	 		continue
	 	}else {
			buf := bytes.Buffer{}
			enc := gob.NewEncoder(&buf)
			enc.Encode(StoreForwardIndex{
				tokenLen: request.DocTokenLen,
				field:    request.Field,
			})
			indexer.dbforwardIndex.Set([]byte(request.DocID), buf.Bytes())
			atomic.AddUint64(&indexer.numDocsStore, 1)
		}
	}
}

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
