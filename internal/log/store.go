package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian // defines the encoding that we persist record sizes and index entries in
)

const (
	lenWidth = 8 // number of bytes used to store the record's length
)

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

// This method creates a Store for the given file
func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	// In case we are re-creating a file with existing data we need to get the size
	// this could happen in case we have a service restart
	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// This method appends the given bytes to the store. We write the length of the record
// so that, when we read the record, we know how many bytes to read. We write to the
// buffered writer instead of directly to the file to reduce the number of system calls
// and improve performance. If the user wrote a lot of small records, this would help a lot.
// Then we return the number of bytes written, which similar to GO APIs conventionally do,
// and the position where the store holds the record in its file. The segment will use this
// position when it creates an associated index entry for the record.
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pos = s.size
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	w += lenWidth
	s.size += uint64(w)
	return uint64(w), pos, nil
}

// This method return the record stored at the given position.
// First it flushes the writer buffer, in case we're about to try to read a
// record that the bufferhasn't flushed to disk yet. We find out how many bytes
// we have to read to get the whole record, and then  we fetch and return the record.
// The compiler allocates byte slices that don't escape the functions they're
// declared in on the stack. A value scapes when it lives beyond the lifetime of
// the function call -- if you return the value, for example
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	b := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return b, nil
}

// This method reads len(p) bytes into the p beginning at the off offset in the store's file.
// It implements io.ReaderAt on the store type
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.File.ReadAt(p, off)
}

// This method persists any buffered data before closing the file.
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}
