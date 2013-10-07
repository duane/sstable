package sstable

import (
  "errors"
  "fmt"
  "io/ioutil"
  "os"
)

const maxVarintBytes = 6

type Key interface {
  Equals(other interface{}) bool
  Less(other interface{}) bool
  MarshalKey() ([]byte, error)
  UnmarshalKey(data []byte) error
  String() string
}

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
  return 0, 0
}

func EncodeBuf(buf []byte) (encoded []byte) {
  encoded = append(EncodeVarint(uint64(len(buf))), buf...)
  return
}

func DecodeBuf(buf []byte, decoded *[]byte) (n int, err error) {
  length, n := DecodeVarint(buf)
  if n == 0 {
    err = fmt.Errorf("Read 0 bytes")
    return
  }

  *decoded = buf[n : length+1]

  n += int(length)
  return
}

type Pair struct {
  Key   Key
  Value []byte
}

func (p *Pair) Encode() (encoded []byte, err error) {
  key_buf, err := p.Key.MarshalKey()
  if err != nil {
    return
  }

  encoded = append(EncodeBuf(key_buf), EncodeBuf(p.Value)...)
  return
}

func (p *Pair) Decode(encoded []byte) (nn uint, err error) {
  key_buf := []byte{}
  n, err := DecodeBuf(encoded, &key_buf)
  if err != nil {
    return
  }
  err = p.Key.UnmarshalKey(key_buf)
  if err != nil {
    return
  }

  nn += uint(n)

  decoded_val := []byte{}
  n, err = DecodeBuf(encoded[n:], &decoded_val)
  if err != nil {
    return
  }
  nn += uint(n)

  p.Value = decoded_val
  return
}

func EncodePairStream(filename string, pair_chan chan *Pair) {
  file, err := os.Create(filename)
  if err != nil {
    panic(err.Error())
    return
  }
  defer file.Close()

  for {
    pair, ok := <-pair_chan
    if !ok {
      break
    }

    fmt.Print(pair.Key.String())
    encoded, err := pair.Encode()
    if err != nil {
      panic(err.Error())
      return
    }

    n, err := file.Write(encoded)
    if err != nil {
      panic(err.Error())
      return
    }

    if n != len(encoded) {
      err = errors.New("Didn't write entire buffer!")
      panic(err.Error())
      return
    }
    pair_chan <- nil
  }
}

func DecodePairStream(key_ctor func() Key, filename string, pair_chan chan *Pair) {
  bytes, err := ioutil.ReadFile(filename)
  if err != nil {
    panic(err.Error())
  }
  defer close(pair_chan)

  for {
    pair := &Pair{Key: key_ctor()}
    n, err := pair.Decode(bytes)
    if err != nil {
      panic(err.Error())
    }

    pair_chan <- pair

    bytes = bytes[n:]
    if len(bytes) == 0 {
      break
    }
  }
}
