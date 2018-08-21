package tdigest

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

const encVer = int16(1)

var header = []byte("tdigest!")

// MarshalBinary serializes the digest as a sequence of bytes suitable for
// deserialization by UnmarshalBinary. The idea for this was taken from
// https://github.com/spenczar/tdigest.
func (t *TDigest) MarshalBinary() ([]byte, error) {
	t.process()

	w := binwriter{}
	w.Write(header)
	w.Write(encVer)
	w.Write(t.Compression)
	w.Write(int32(t.maxProcessed))
	w.Write(int32(t.maxUnprocessed))
	w.Write(t.processedWeight)
	w.Write(t.min)
	w.Write(t.max)
	w.Write(int32(len(t.cumulative)))
	for _, c := range t.cumulative {
		w.Write(c)
	}
	w.Write(int32(t.processed.Len()))
	for _, c := range t.processed {
		w.Write(c.Mean)
		w.Write(c.Weight)
	}
	if w.err != nil {
		return nil, w.err
	}
	return w.w.Bytes(), nil
}

// UnmarshalBinary populates the TDigest t with the parsed data from p which
// should have been created with MarshalBinary.
func (t *TDigest) UnmarshalBinary(p []byte) error {
	if len(p) == 0 {
		return nil
	}

	var (
		head = make([]byte, len(header))
		enc  int16
	)
	r := binreader{r: bytes.NewReader(p)}
	r.Read(&head)
	if !bytes.Equal(head, header) {
		return fmt.Errorf("tdigest: invalid file header \"%v\"", head)
	}

	r.Read(&enc)
	// TODO: Handle encoding versions in a backwards compatible way
	if enc != encVer {
		return fmt.Errorf("tdigest: unhandled encoding version %v", enc)
	}

	var (
		n int32
		c float64
	)

	r.Read(&t.Compression)
	r.Read(&n)
	t.maxProcessed = int(n)
	r.Read(&n)
	t.maxUnprocessed = int(n)
	r.Read(&t.processedWeight)
	t.unprocessedWeight = 0
	r.Read(&t.min)
	r.Read(&t.max)

	r.Read(&n)
	t.cumulative = make([]float64, n)
	for i := 0; i < int(n); i++ {
		r.Read(&c)
		t.cumulative[i] = c
	}

	r.Read(&n)
	t.processed = make([]Centroid, n, t.maxProcessed)
	for i := 0; i < int(n); i++ {
		centroid := Centroid{}
		r.Read(&c)
		centroid.Mean = c
		r.Read(&c)
		centroid.Weight = c

		t.processed[i] = centroid
	}

	r.Read(&n)
	t.unprocessed = make([]Centroid, 0, t.maxUnprocessed+1)
	return r.err
}

type binwriter struct {
	w   bytes.Buffer
	err error
}

func (w *binwriter) Write(v interface{}) {
	if w.err != nil {
		return
	}
	w.err = binary.Write(&w.w, binary.LittleEndian, v)
}

type binreader struct {
	r   io.Reader
	err error
}

func (r binreader) Read(dst interface{}) {
	if r.err != nil {
		return
	}
	r.err = binary.Read(r.r, binary.LittleEndian, dst)
	if r.err == io.EOF {
		r.err = io.ErrUnexpectedEOF
	}
}
