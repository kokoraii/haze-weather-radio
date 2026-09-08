package datastore

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func TestCAPArchiveConcurrentRoundTrips(t *testing.T) {
	for i := range 16 {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			raw := []byte(fmt.Sprintf("<alert><identifier>%d</identifier>%s</alert>", i, strings.Repeat("weather bulletin ", 100)))
			for range 8 {
				archive, err := EncodeCAPXMLArchive(raw)
				if err != nil {
					t.Fatal(err)
				}
				decoded, err := DecodeCAPXMLArchive(archive.Compressed)
				if err != nil || !bytes.Equal(decoded, raw) {
					t.Fatalf("archive round trip: err=%v, matches=%v", err, bytes.Equal(decoded, raw))
				}
			}
		})
	}
}

func BenchmarkCAPArchiveEncode(b *testing.B) {
	raw := []byte("<alert>" + strings.Repeat("<info><event>thunderstorm</event><description>Weather warning for the forecast region.</description></info>", 100) + "</alert>")
	if _, err := EncodeCAPXMLArchive(raw); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	for range b.N {
		if _, err := EncodeCAPXMLArchive(raw); err != nil {
			b.Fatal(err)
		}
	}
}
