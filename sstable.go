package sstable

import (
  "fmt"
)

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

func DecodeVarint(buf []byte) (x uint64, n int) {
  // x, n already 0
  for shift := uint(0); shift < 64; shift += 7 {
    if n >= len(buf) {
      return 0, 0
    }
    b := uint64(buf[n])
    n++
    x |= (b & 0x7F) << shift
    if (b & 0x80) == 0 {
      return x, n
    }
  }

  // The number is too large to represent in a 64-bit value.
  return 0, 0
}

func EncodePair(key string, value []byte) ([]byte, error) {
  key_length_bytes := EncodeVarint(uint64(len(key)))
  value_length_bytes := EncodeVarint(uint64(len(value)))
  encoded_key := append(key_length_bytes, []byte(key)...)
  encoded_value := append(value_length_bytes, value...)
  encoded_pair := append(encoded_key, encoded_value...)
  return encoded_pair, nil
}

func DecodeBuf(buf []byte) (decoded []byte, err error) {
  length, read := DecodeVarint(buf)
  if read == 0 {
    err = fmt.Errorf("Read 0 bytes")
    return
  }
  decoded = buf[read:length]
  return
}

func DecodePair(buf []byte) error {
  key, err := DecodeBuf(buf)
  if err != nil {
    return err
  }

  value, err := DecodeBuf(buf[len(key):])
  if err != nil {
    return err
  }

  fmt.Printf("Key: %v, value: %v", key, value)
  return nil
}

func Flush(table *MemTable, filename string) error {
  return nil
}

type SSTableSet []SSTableRef

type SSTableRef struct {
  filename string
}
