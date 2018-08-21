package tdigest_test

import (
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
