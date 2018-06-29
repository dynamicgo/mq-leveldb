package leveldb

import (
	"encoding/binary"
	"encoding/json"

	"github.com/dynamicgo/slf4go"

	"github.com/dynamicgo/mq"
	goleveldb "github.com/syndtr/goleveldb/leveldb"
)

type producerImpl struct {
	slf4go.Logger
	db *goleveldb.DB
}

func newProducer(db *goleveldb.DB) (*producerImpl, error) {
	producer := &producerImpl{
		Logger: slf4go.Get("mq-leveldb-producer"),
		db:     db,
	}

	return producer, nil
}

func (producer *producerImpl) getOffset(transaction *goleveldb.Transaction) (int64, error) {
	buff, err := transaction.Get(offsetKey, nil)

	if err != nil {
		return 0, err
	}

	offset := int64(binary.BigEndian.Uint64(buff))

	return offset, nil
}

func (producer *producerImpl) Record(key []byte, value []byte) (mq.Record, error) {
	return &recordImpl{
		Joffset: -1,
		Jkey:    key,
		Jvalue:  value,
	}, nil
}

func (producer *producerImpl) Send(record mq.Record) error {

	producer.DebugF("send record ...")

	trans, err := producer.db.OpenTransaction()

	if err != nil {
		return err
	}

	offset, err := producer.getOffset(trans)

	if err != nil {
		producer.ErrorF("get offset err %s", err)
		trans.Discard()
		return err
	}

	data, err := json.Marshal(record)

	if err != nil {
		producer.ErrorF("marshal record err %s", err)
		trans.Discard()
		return err
	}

	if err := trans.Put(keyToBytes(offset), data, nil); err != nil {
		producer.ErrorF("insert record err %s", err)
		trans.Discard()
		return err
	}

	offset++

	if offset < 0 {
		offset = 0
	}

	if err := updateIndexer(trans, offsetKey, offset); err != nil {
		producer.ErrorF("insert record err %s", err)
		trans.Discard()
		return err
	}

	if err := trans.Commit(); err != nil {
		producer.DebugF("send record err %s", err)
		return err
	}

	producer.DebugF("send record -- success")

	return nil
}

func (producer *producerImpl) Batch(records []mq.Record) error {

	trans, err := producer.db.OpenTransaction()

	if err != nil {
		return err
	}

	offset, err := producer.getOffset(trans)

	if err != nil {
		producer.ErrorF("get offset err %s", err)
		trans.Discard()
		return err
	}

	batch := new(goleveldb.Batch)

	for _, record := range records {
		data, err := json.Marshal(record)

		if err != nil {
			producer.ErrorF("marshal record err %s", err)
			trans.Discard()
			return err
		}

		batch.Put(keyToBytes(offset), data)

		offset++

		if offset < 0 {
			offset = 0
		}
	}

	if err := trans.Write(batch, nil); err != nil {
		producer.ErrorF("insert records err %s", err)
		trans.Discard()
		return err
	}

	if err := updateIndexer(trans, offsetKey, offset); err != nil {
		producer.ErrorF("update indexer err %s", err)
		trans.Discard()
		return err
	}

	return trans.Commit()
}
