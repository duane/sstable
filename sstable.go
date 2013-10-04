package sstable

type MemTable map[string][]byte

const maxVarintBytes = 6

func EncodeVarint(x uint64) []byte {
  var buf [maxVarintBytes]byte
  var n int
  for n = 0; x > 127; n++ {
    buf[n] = 0x80 | uint8(x&0x7F)
    x >>= 7
  }
  buf[n] = uint8(x)
  n++
  return buf[0:n]
}

func EncodePair(key string, value []byte) ([]byte, error) {
  key_length_bytes := EncodeVarint(uint64(len(key)))
  value_length_bytes := EncodeVarint(uint64(len(value)))
  encoded_key := append(key_length_bytes, []byte(key)...)
  encoded_value := append(value_length_bytes, value...)
  encoded_pair := append(encoded_key, encoded_value...)
  return encoded_pair, nil
}

func Flush(table *MemTable, filename string) error {
  return nil
}

type SSTableSet []SSTableRef

type SSTableRef struct {
  filename string
}
