package datastore

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// Stateless zstd calls are concurrency-safe. Keep one reusable codec workspace
// per direction instead of allocating encoder tables for every feed/archive row.
var capArchiveEncoder = sync.OnceValues(func() (*zstd.Encoder, error) {
	return zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBetterCompression), zstd.WithEncoderConcurrency(1))
})

var capArchiveDecoder = sync.OnceValues(func() (*zstd.Decoder, error) {
	return zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
})

// EncodeCAPXMLArchive compresses a CAP document and records its integrity hash.
func EncodeCAPXMLArchive(raw []byte) (CAPXMLArchive, error) {
	if len(raw) == 0 {
		return CAPXMLArchive{}, nil
	}
	encoder, err := capArchiveEncoder()
	if err != nil {
		return CAPXMLArchive{}, fmt.Errorf("create zstd encoder: %w", err)
	}
	hash := sha256.Sum256(raw)
	compressed := encoder.EncodeAll(raw, nil)
	return CAPXMLArchive{
		Compressed: compressed,
		SHA256Hex:  hex.EncodeToString(hash[:]),
		RawBytes:   len(raw),
		ZstdBytes:  len(compressed),
	}, nil
}

// DecodeCAPXMLArchive reads existing and newly encoded zstd CAP archives.
func DecodeCAPXMLArchive(compressed []byte) ([]byte, error) {
	decoder, err := capArchiveDecoder()
	if err != nil {
		return nil, fmt.Errorf("create zstd decoder: %w", err)
	}
	return decoder.DecodeAll(compressed, nil)
}
