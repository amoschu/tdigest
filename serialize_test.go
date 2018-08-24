package tdigest_test

import (
	"bytes"
	"compress/gzip"
	"testing"

	"github.com/amoschu/tdigest"
)

func TestTdigest_MarshalUnmarshal(t *testing.T) {
	n := 100
	uniformQ := make([]float64, n)
	normalQ := make([]float64, n)
	for i := 0; i < n; i++ {
		uniformQ[i] = UniformDigest.Quantile(float64(i) / 100)
		normalQ[i] = NormalDigest.Quantile(float64(i) / 100)
	}

	uniformBytes, err := UniformDigest.MarshalBinary()
	if err != nil {
		t.Fatalf("failed to marshal uniform digest (%s)", err)
	}
	normalBytes, err := NormalDigest.MarshalBinary()
	if err != nil {
		t.Fatalf("failed to marshal normal digest (%s)", err)
	}

	tuniform := tdigest.NewWithCompression(420)
	tnormal := tdigest.NewWithCompression(69)
	if err = tuniform.UnmarshalBinary(uniformBytes); err != nil {
		t.Fatalf("failed to unmarshal uniform digest (%s)", err)
	}
	if err = tnormal.UnmarshalBinary(normalBytes); err != nil {
		t.Fatalf("failed to unmarshal normal digest (%s)", err)
	}

	if tuniform.Compression != UniformDigest.Compression {
		t.Errorf("unmarshalled uniform digest compression=%f, expected %f",
			tuniform.Compression, UniformDigest.Compression,
		)
	}
	if tnormal.Compression != NormalDigest.Compression {
		t.Errorf("unmarshalled uniform digest compression=%f, expected %f",
			tnormal.Compression, NormalDigest.Compression,
		)
	}

	for i := 0; i < n; i++ {
		if tuniform.Quantile(float64(i)/100) != uniformQ[i] {
			t.Errorf("have unmarshalled uniform %f quantile: %f, expected %f",
				float64(i), tuniform.Quantile(float64(i)/100), uniformQ[i],
			)
		}
		if tnormal.Quantile(float64(i)/100) != normalQ[i] {
			t.Errorf("have unmarshalled normal %f quantile: %f, expected %f",
				float64(i), tnormal.Quantile(float64(i)/100), normalQ[i],
			)
		}
	}
}

var benchResult interface{}

func BenchmarkTDigest_MarshalBinary(b *testing.B) {
	for n := 0; n < b.N; n++ {
		benchResult, _ = NormalDigest.MarshalBinary()
	}
}

func BenchmarkTDigest_UnmarshalBinary(b *testing.B) {
	d := tdigest.NewWithCompression(420)
	p, err := NormalDigest.MarshalBinary()
	if err != nil {
		b.Fatalf("failed to marshal NormalDigest (%s)", err)
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		benchResult = d.UnmarshalBinary(p)
	}
}

func BenchmarkTDigest_MarshalBinary_GzipDefaultCompression(b *testing.B) {
	p, err := NormalDigest.MarshalBinary()
	buf := bytes.Buffer{}
	buf.Grow(len(p))
	w := gzip.NewWriter(&buf)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		buf.Reset()
		w.Reset(&buf)
		b.StartTimer()

		p, err = NormalDigest.MarshalBinary()
		if err != nil {
			b.Fatalf("failed to marshal NormalDigest (%s)", err)
		}
		benchResult, _ = w.Write(p)
		benchResult = w.Flush()
	}
}
