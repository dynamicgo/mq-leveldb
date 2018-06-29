package leveldb

import (
	"testing"
	"time"

	"github.com/dynamicgo/slf4go"

	"github.com/stretchr/testify/require"

	"baas.tech/wallet/mq"

	config "github.com/dynamicgo/go-config"
	"github.com/dynamicgo/go-config/source/memory"
)

var configdata = `{
    "leveldb":{
		"broker":"../../.test",
		"topic":"levemq",
		"consumer":"test"
	}
}`

var conf config.Config

func init() {
	conf = config.NewConfig()

	err := conf.Load(memory.NewSource(
		memory.WithData([]byte(configdata)),
	))

	if err != nil {
		panic(err)
	}
}

func TestProducer(t *testing.T) {
	_, err := mq.NewProducer("leveldb", conf)

	require.NoError(t, err)
}

func TestConsumer(t *testing.T) {
	_, err := mq.NewConsumer("leveldb", conf)

	require.NoError(t, err)
}

func TestConsumerBlance(t *testing.T) {
	producer, err := mq.NewProducer("leveldb", conf)

	require.NoError(t, err)

	consumer, err := mq.NewConsumer("leveldb", conf)

	require.NoError(t, err)

	consumer2, err := mq.NewConsumer("leveldb", conf)

	require.NoError(t, err)

	record, err := producer.Record([]byte("test"), []byte("test"))

	require.NoError(t, err)

	require.NoError(t, producer.Send(record))

	record2 := <-consumer.Recv()

	consumer.Commit(record2)

	select {
	case <-consumer2.Recv():
		require.True(t, false, "not here")
	case <-time.NewTimer(time.Second * 10).C:

	}
}

func TestSendRecvMessage(t *testing.T) {
	producer, err := mq.NewProducer("leveldb", conf)

	require.NoError(t, err)

	consumer, err := mq.NewConsumer("leveldb", conf)

	require.NoError(t, err)

	record, err := producer.Record([]byte("test"), []byte("test"))

	require.NoError(t, err)

	require.NoError(t, producer.Send(record))

	for record2 := range consumer.Recv() {
		require.Equal(t, record.Key(), record2.Key())
		require.Equal(t, record.Value(), record2.Value())

		consumer.Commit(record2)

		// require.NoError(t, producer.Send(record))

		records := []mq.Record{
			record,
		}

		require.NoError(t, producer.Batch(records))
	}

}

func BenchmarkConsumer(b *testing.B) {
	b.StopTimer()
	slf4go.SetLevel(0)
	producer, err := mq.NewProducer("leveldb", conf)

	require.NoError(b, err)

	record, err := producer.Record([]byte("test"), []byte("test"))

	require.NoError(b, err)

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		producer.Send(record)
	}

	slf4go.SetLevel(slf4go.Debug)
}
