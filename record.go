package leveldb

type recordImpl struct {
	Joffset int64  `json:"-"`
	Jkey    []byte `json:"key"`
	Jvalue  []byte `json:"value"`
}

func (record *recordImpl) Key() []byte {
	return record.Jkey
}

func (record *recordImpl) Value() []byte {
	return record.Jvalue
}
