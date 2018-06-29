package leveldb

import (
	"encoding/json"
	"time"

	"baas.tech/wallet/mq"
	"github.com/dynamicgo/slf4go"
	goleveldb "github.com/syndtr/goleveldb/leveldb"
)

func addConsumer(consumers []string, consumer string) []string {
	for _, c := range consumers {
		if c == consumer {
			return consumers
		}
	}

	return append(consumers, consumer)
}

func consumerToKey(consumer string) []byte {
	return append(consumerKey, []byte(consumer)...)
}

type consumerImpl struct {
	slf4go.Logger
	db      *goleveldb.DB
	recvq   chan mq.Record
	errorsq chan error
	key     []byte
	topic   string
	name    string
}

func newConsumer(db *goleveldb.DB, topic, name string, cached int) (mq.Consumer, error) {
	consumer := &consumerImpl{
		Logger:  slf4go.Get("mq-leveldb-consumer"),
		db:      db,
		recvq:   make(chan mq.Record, cached),
		errorsq: make(chan error, cached),
		key:     consumerToKey(name),
		name:    name,
		topic:   topic,
	}

	trans, err := db.OpenTransaction()

	if err != nil {
		return nil, err
	}

	if err := consumer.addIndexer(trans); err != nil {
		consumer.ErrorF("add consumer indexer err %s", err)
		trans.Discard()
		return nil, err
	}

	if err := consumer.initIndexer(trans); err != nil {
		trans.Discard()
		consumer.ErrorF("init consumer indexer err %s", err)
		return nil, err
	}

	if err := trans.Commit(); err != nil {
		return nil, err
	}

	go consumer.run(time.Second*4, cached)

	return consumer, nil
}

func (consumer *consumerImpl) initIndexer(trans *goleveldb.Transaction) error {
	// _, err := trans.Has(consumer.key, nil)

	// if err != nil {
	// 	return err
	// }

	compactIndexer, err := getIndexer(trans, compactKey)

	if err != nil {
		return err
	}

	if err := updateIndexer(trans, consumer.key, compactIndexer); err != nil {
		return err
	}

	return nil
}

func (consumer *consumerImpl) addIndexer(trans *goleveldb.Transaction) error {

	var consumers []string

	ok, err := trans.Has(consumerKey, nil)

	if err != nil {
		return nil
	}

	if ok {
		buff, err := trans.Get(consumerKey, nil)

		if err != nil {
			return err
		}

		if err := json.Unmarshal(buff, &consumers); err != nil {
			return err
		}
	}

	consumers = addConsumer(consumers, consumer.name)

	buff, err := json.Marshal(consumers)

	if err != nil {
		return err
	}

	return trans.Put(consumerKey, buff, nil)
}

func (consumer *consumerImpl) run(duration time.Duration, cached int) {
	ticker := time.NewTicker(duration)
	defer ticker.Stop()

	var cachedOffset int64

	for range ticker.C {
		trans, err := consumer.db.OpenTransaction()
		if err != nil {
			consumer.ErrorF("consumer pull create leveldb transaction err %s", err)
			continue
		}

		offset, err := getIndexer(trans, offsetKey)

		if err != nil {
			consumer.ErrorF("consumer get producer offset err %s", err)
			trans.Discard()
			continue
		}

		consumerOffset, err := getIndexer(trans, consumer.key)

		if err != nil {
			consumer.ErrorF("consumer get consumer offset err %s", err)
			trans.Discard()
			continue
		}

		consumer.DebugF("consumer(%s:%s) poll: offset(%d) consumer offset(%d) cached offset(%d)", consumer.topic, consumer.name, offset, consumerOffset, cachedOffset)

		if cachedOffset > consumerOffset {
			consumerOffset = cachedOffset
		}

		if consumerOffset >= offset {
			trans.Discard()
			continue
		}

		loadsize := offset - consumerOffset

		if loadsize > int64(cached) {
			loadsize = int64(cached)
		}

		records := make([]mq.Record, 0)

		for i := int64(0); i < loadsize; i++ {
			data, err := trans.Get(keyToBytes(i+consumerOffset), nil)

			if err != nil {
				consumer.ErrorF("consumer(%s:%s) get record err %s", consumer.topic, consumer.name, err)
				trans.Discard()
				goto SEND
			}

			var record *recordImpl

			if err := json.Unmarshal(data, &record); err != nil {
				consumer.ErrorF("consumer(%s:%s) get record err %s", consumer.topic, consumer.name, err)
				trans.Discard()
				goto SEND
			}

			record.Joffset = i + consumerOffset

			records = append(records, record)
		}

		if err := trans.Commit(); err != nil {
			consumer.ErrorF("consumer(%s:%s) commmit err %s", consumer.topic, consumer.name, err)
			continue
		}

	SEND:

		consumer.DebugF("consumer(%s:%s) poll records(%d)", consumer.topic, consumer.name, len(records))

		for _, record := range records {
			consumer.recvq <- record
		}

		cachedOffset = consumerOffset + int64(len(records))

	}
}

func (consumer *consumerImpl) Recv() <-chan mq.Record {
	return consumer.recvq
}

func (consumer *consumerImpl) Errors() <-chan error {
	return consumer.errorsq
}

func (consumer *consumerImpl) Commit(record mq.Record) error {
	r, ok := record.(*recordImpl)

	if !ok {
		panic("input record is not leveldb version record")
	}

	trans, err := consumer.db.OpenTransaction()

	if err != nil {
		consumer.ErrorF("commit error %s", err)
		return err
	}

	offset, err := getIndexer(trans, consumer.key)

	if offset > r.Joffset {
		trans.Discard()
		consumer.ErrorF("skip commit %d %d", offset, r.Joffset)
		return nil
	}

	offset = r.Joffset

	offset++

	if offset < 0 {
		offset = 0
	}

	if err := updateIndexer(trans, consumer.key, offset); err != nil {
		trans.Discard()
		consumer.ErrorF("commit error %s", err)
		return err
	}

	if err := trans.Commit(); err != nil {
		return err
	}

	consumer.TraceF("consumer offset set to %d", offset)

	return nil
}
