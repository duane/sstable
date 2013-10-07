package sstable

import (
  "bytes"
  "encoding/hex"
  "log"
  "testing"
)

func TestEncodeDecode(t *testing.T) {
  test_data := map[string][]byte{
    "1520235a-0986-4d82-abc0-cf2482c69f73": []byte("32610c7f-f99c-46c1-91fc-8b0830bd596f"),
    "1a119fd8-bfbc-46fc-bf99-18eeb3615877": []byte("ffc1a6b7-e325-4174-a552-0aedf135bba0"),
    "008acb4e-2e51-4584-a702-89458ded7dec": []byte("870df4b0-aa09-4c18-8c4b-965b25dd7e20"),
    "9594d23e-075b-44b3-8149-94930b615176": []byte("bfb2472b-5549-4f6d-85d0-5274de57527c"),
    "6bf772fa-d018-44a8-8ca9-c654b18f5fbd": []byte("941b8391-1354-4d05-afa1-7656bf23b240"),
    "a6ffe57d-2c61-4663-b4fc-496c5d012f3f": []byte("88c8a437-afd4-4224-921e-b285e7ed98bc"),
    "6a3348dd-e434-486f-9b34-f3b94472fedf": []byte("52cc6343-2894-4a64-9d96-70efa8e7d154"),
    "2c3ec29e-b780-48d6-a260-3a0040b70401": []byte("8624e638-7a98-474f-8718-7308d266cea6"),
    "a8a62fcc-ba6a-4068-acfe-90a220cec6e2": []byte("f924beba-4c8d-47ca-beb9-f763865e933c"),
    "3d52b6b0-8cc8-47e4-b435-ecde7c599ff7": []byte("f9b36755-055a-4f58-aad9-39c8ed6e2092"),
    "76b09f51-4c9a-4f10-b447-6a0fc99f8cc5": []byte("76c8838d-396c-4bf0-a19d-a089f7a80d5a"),
    "e367623f-9108-477e-980c-934ccd85793d": []byte("aa2a31aa-b1d3-46f2-b779-05d170aac45d"),
    "927e0b7c-c2fc-4de5-80a3-f55552e88829": []byte("e7b10703-4c79-4e9a-91de-d502bab45ab1"),
    "23ca0baf-873d-48d9-bad4-eddb30d4e484": []byte("d4265684-787d-4dea-8bdf-dfaae87aedd3"),
    "0f205d0e-7f05-497f-9e8c-bde7b77b7416": []byte("728eae36-4043-4506-a8d1-8153062a37f0"),
    "1b0ade7d-bcc7-46db-b01b-3a0175023be4": []byte("53be47a2-2344-49d7-b6fc-f9618f146a50"),
    "5d4876e6-575d-48d1-9397-d669695fff65": []byte("f512fccd-cb4d-47a8-ab8f-be7f200e689a"),
    "a58998c5-3ee7-459f-b90f-2ab3c1d5fb49": []byte("a50bbeed-6546-49bd-98a9-98772cebd0f9"),
    "d9a4b3a3-2087-4df2-aea7-2f4fada2b27a": []byte("f0521f70-6544-42a3-b768-74747d7f8b3d"),
    "3b508d13-dbbc-498d-bb42-f82405a1e686": []byte("6e97ad28-f0e8-431b-8f77-22d126bacb61"),
  }

  pairs := []*Pair{}
  for k, v := range test_data {
    pairs = append(pairs, &Pair{Key: []byte(k), Value: v})
  }

  pair := pairs[0]
  encoded, err := pair.Encode()
  if err != nil {
    t.Fail()
  }

  log.Print(hex.EncodeToString(encoded))

  pair2 := &Pair{}
  err = pair2.Decode(encoded)
  if err != nil {
    t.Fail()
  }

  if !bytes.Equal(pair.Key, pair2.Key) {
    t.Fatalf("%d vs %d", len(pair.Key), len(pair2.Key))
  } else if !bytes.Equal(pair.Value, pair2.Value) {
    t.Fatalf("%v vs %v", pair.Value, pair2.Value)
  }
}
