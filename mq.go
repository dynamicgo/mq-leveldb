package leveldb

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/dynamicgo/slf4go"

	"github.com/dynamicgo/go-config"
	"github.com/dynamicgo/mq"
	goleveldb "github.com/syndtr/goleveldb/leveldb"
)

var (
	offsetKey   = []byte("06195743185681766170876958057592")
	consumerKey = []byte("16195743185681766170876958057592")
	compactKey  = []byte("26195743185681766170876958057592")
)

func keyToBytes(key int64) []byte {
	var buff [8]byte

	binary.BigEndian.PutUint64(buff[:], uint64(key))

	return buff[:]
}

func bytesToKey(buff []byte) int64 {
	return int64(binary.BigEndian.Uint64(buff))
}

func getIndexer(transaction *goleveldb.Transaction, key []byte) (int64, error) {

	ok, err := transaction.Has(key, nil)

	if err != nil {
		return 0, err
	}

	if !ok {
		return 0, nil
	}

	buff, err := transaction.Get(key, nil)

	if err != nil {
		return 0, err
	}

	return bytesToKey(buff), nil
}

func updateIndexer(transaction *goleveldb.Transaction, key []byte, offset int64) error {
	buff := keyToBytes(offset)

	return transaction.Put(key, buff, nil)
}

type brokerImpl struct {
	sync.RWMutex
	slf4go.Logger
	db              map[string]*goleveldb.DB
	producer        map[string]*producerImpl
	consumer        map[string]mq.Consumer
	compactDuration time.Duration
}

func newBorker() *brokerImpl {
	broker := &brokerImpl{
		Logger:          slf4go.Get("mq-leveldb"),
		db:              make(map[string]*goleveldb.DB),
		producer:        make(map[string]*producerImpl),
		consumer:        make(map[string]mq.Consumer),
		compactDuration: time.Second * 10,
	}

	go broker.compactMQ()

	return broker
}

func (broker *brokerImpl) compactMQ() {
	ticker := time.NewTicker(broker.compactDuration)

	defer ticker.Stop()

	broker.processCompact()

	for range ticker.C {
		broker.processCompact()
	}
}

func (broker *brokerImpl) processCompact() {

	broker.DebugF("process compact ...")

	broker.RLock()
	dbs := make(map[string]*goleveldb.DB)

	for name, db := range broker.db {
		dbs[name] = db
	}

	broker.RUnlock()

	broker.DebugF("process compact db(%d)", len(dbs))

	for name, db := range dbs {
		tran, err := db.OpenTransaction()
		if err != nil {
			broker.ErrorF("open trans for cmpact err %s", err)
			continue
		}

		if err := broker.compactDB(name, tran); err != nil {
			broker.ErrorF("compactDB err %s", err)
			tran.Discard()
			continue
		}

		tran.Commit()
	}

	broker.DebugF("process compact -- finish")
}

func (broker *brokerImpl) compactDB(name string, trans *goleveldb.Transaction) error {
	ok, err := trans.Has(consumerKey, nil)

	if err != nil {
		return err
	}

	if !ok {
		return nil
	}

	buff, err := trans.Get(consumerKey, nil)

	if err != nil {
		return err
	}

	var consumers []string

	if err := json.Unmarshal(buff, &consumers); err != nil {
		return err
	}

	compactIndexer, err := getIndexer(trans, compactKey)

	if err != nil {
		return err
	}

	consumerIndexer := int64(-1)

	var indexers []string

	for _, consumer := range consumers {
		indexer, err := getIndexer(trans, consumerToKey(consumer))

		if err != nil {
			return err
		}

		if (indexer < consumerIndexer && indexer >= compactIndexer) || consumerIndexer == int64(-1) {
			consumerIndexer = indexer
		}

		indexers = append(indexers, fmt.Sprintf("%d", indexer))
	}

	if consumerIndexer == int64(-1) {
		return nil
	}

	broker.TraceF("compact %s db (%s) (%s)", name, strings.Join(consumers, ","), strings.Join(indexers, ","))
	broker.DebugF("compact %s db(%d,%d)", name, compactIndexer, consumerIndexer)

	for i := compactIndexer; i < consumerIndexer; i++ {
		if err := trans.Delete(keyToBytes(i), nil); err != nil {
			return err
		}
	}

	return updateIndexer(trans, compactKey, consumerIndexer)
}

func (broker *brokerImpl) NewConsumer(config config.Config) (mq.Consumer, error) {
	brokerName := config.Get("broker").String("./leveldbmq")
	topic := config.Get("topic").String("leveldb")
	consumerName := config.Get("consumer").String("consumer")
	cached := config.Get("cached").Int(100)

	broker.DebugF("create consumer %s:%s:%s", consumerName, topic, brokerName)

	dbpath := filepath.Join(brokerName, topic)

	consumerPath := filepath.Join(brokerName, topic, consumerName)

	broker.Lock()
	defer broker.Unlock()

	consumer, ok := broker.consumer[consumerPath]

	if ok {
		return consumer, nil
	}

	db, ok := broker.db[dbpath]

	if !ok {

		broker.DebugF("create broker %s backend leveldb %s", brokerName, dbpath)

		var err error
		db, err = goleveldb.OpenFile(dbpath, nil)

		if err != nil {
			broker.ErrorF("create leveldb %s err %s", dbpath, err)
			return nil, err
		}

		if err := initdb(db); err != nil {
			broker.ErrorF("create leveldb %s err %s", dbpath, err)
			return nil, err
		}

		broker.db[dbpath] = db

		broker.DebugF("create broker %s backend leveldb %s -- success", brokerName, dbpath)
	}

	consumer, err := newConsumer(db, topic, consumerName, cached)

	if err != nil {
		broker.ErrorF("create consumer %s err %s", topic, err)
		return nil, err
	}

	broker.consumer[consumerPath] = consumer

	broker.DebugF("create consumer %s:%s:%s -- success", consumerName, topic, brokerName)

	return consumer, nil
}

func (broker *brokerImpl) NewProducer(config config.Config) (mq.Producer, error) {
	brokerName := config.Get("broker").String("./leveldbmq")
	topic := config.Get("topic").String("leveldb")

	broker.DebugF("create producer %s:%s", topic, brokerName)

	dbpath := filepath.Join(brokerName, topic)

	broker.Lock()
	defer broker.Unlock()

	producer, ok := broker.producer[dbpath]

	if ok {
		return producer, nil
	}

	db, ok := broker.db[dbpath]

	if !ok {

		broker.DebugF("create broker %s backend leveldb %s", brokerName, dbpath)

		var err error
		db, err = goleveldb.OpenFile(dbpath, nil)

		if err != nil {
			broker.ErrorF("create leveldb %s err %s", dbpath, err)
			return nil, err
		}

		if err := initdb(db); err != nil {
			broker.ErrorF("create leveldb %s err %s", dbpath, err)
			return nil, err
		}

		broker.db[dbpath] = db

		broker.DebugF("create broker %s backend leveldb %s -- success", brokerName, dbpath)
	}

	producer, err := newProducer(db)

	if err != nil {
		broker.ErrorF("create producer %s err %s", topic, err)
		return nil, err
	}

	broker.producer[dbpath] = producer

	broker.DebugF("create producer %s:%s -- success", topic, brokerName)

	return producer, nil
}

func initdb(db *goleveldb.DB) error {

	var buff [8]byte

	ok, err := db.Has(offsetKey, nil)

	if err != nil {
		return err
	}

	if ok {
		return nil
	}

	binary.BigEndian.PutUint64(buff[:], 0)

	return db.Put(offsetKey, buff[:], nil)
}

var broker *brokerImpl

var once sync.Once

func init() {
	// register driver
	mq.OpenDriver("leveldb", func(config config.Config) (mq.Consumer, error) {

		once.Do(func() {
			broker = newBorker()
		})

		return broker.NewConsumer(config)

	}, func(config config.Config) (mq.Producer, error) {

		once.Do(func() {
			broker = newBorker()
		})

		return broker.NewProducer(config)

	})
}
