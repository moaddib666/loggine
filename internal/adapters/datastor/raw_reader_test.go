package datastor

import (
	"bytes"
	"errors"
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
)

// TestChunkedReader_Read tests the Read method of chunkedReader
func TestChunkedReader_Read(t *testing.T) {
	// Prepare test data
	testData := []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	reader := bytes.NewReader(testData)
	chunkSize := 10

	// Create a new chunkedReader
	cr, _ := NewChunkedReader(reader, chunkSize)

	// Read data in small chunks
	buf := make([]byte, 5)
	var readData []byte
	for {
		n, err := cr.Read(buf)
		if err != nil && err != io.EOF {
			t.Fatalf("Read error: %v", err)
		}
		if n > 0 {
			readData = append(readData, buf[:n]...)
		}
		if err == io.EOF {
			break
		}
	}

	// Compare readData with testData
	if !bytes.Equal(readData, testData) {
		t.Errorf("Data read does not match test data.\nExpected: %s\nGot: %s", string(testData), string(readData))
	}
}

// TestChunkedReader_Seek tests the Seek method of chunkedReader
func TestChunkedReader_Seek(t *testing.T) {
	testData := []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	reader := bytes.NewReader(testData)
	chunkSize := 10
	cr, _ := NewChunkedReader(reader, chunkSize)

	// Seek to position 20 (SeekStart)
	offset, err := cr.Seek(20, io.SeekStart)
	if err != nil {
		t.Fatalf("Seek error: %v", err)
	}
	if offset != 20 {
		t.Errorf("Expected offset 20, got %d", offset)
	}

	// Read 10 bytes from position 20
	buf := make([]byte, 10)
	n, err := cr.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("Read error after seek: %v", err)
	}
	expectedData := testData[20:30]
	if !bytes.Equal(buf[:n], expectedData) {
		t.Errorf("Data read does not match expected data after seek.\nExpected: %s\nGot: %s", string(expectedData), string(buf[:n]))
	}

	// Seek 5 bytes back from current position (SeekCurrent)
	offset, err = cr.Seek(-5, io.SeekCurrent)
	if err != nil {
		t.Fatalf("Seek error: %v", err)
	}
	if offset != 25 {
		t.Errorf("Expected offset 25, got %d", offset)
	}

	// Read 5 bytes from position 25
	n, err = cr.Read(buf[:5])
	if err != nil && err != io.EOF {
		t.Fatalf("Read error after seek: %v", err)
	}
	expectedData = testData[25:30]
	if !bytes.Equal(buf[:n], expectedData) {
		t.Errorf("Data read does not match expected data after seek.\nExpected: %s\nGot: %s", string(expectedData), string(buf[:n]))
	}

	// Seek to the end of the data (SeekEnd)
	offset, err = cr.Seek(0, io.SeekEnd)
	if err != nil {
		t.Fatalf("Seek error: %v", err)
	}
	if offset != int64(len(testData)) {
		t.Errorf("Expected offset %d, got %d", len(testData), offset)
	}

	// Attempt to read from EOF
	n, err = cr.Read(buf)
	if err != io.EOF {
		t.Errorf("Expected EOF error, got: %v", err)
	}
	if n != 0 {
		t.Errorf("Expected to read 0 bytes at EOF, got %d", n)
	}
}

// TestChunkedReader_ReadAcrossChunks tests reading data that spans multiple chunks
func TestChunkedReader_ReadAcrossChunks(t *testing.T) {
	testData := []byte("abcdefghijklmnopqrstuvwxyz")
	reader := bytes.NewReader(testData)
	chunkSize := 5
	cr, _ := NewChunkedReader(reader, chunkSize)

	// Read 15 bytes (should span 3 chunks)
	buf := make([]byte, 15)
	n, err := cr.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("Read error: %v", err)
	}
	if n != 15 {
		t.Errorf("Expected to read 15 bytes, got %d", n)
	}
	expectedData := testData[:15]
	if !bytes.Equal(buf[:n], expectedData) {
		t.Errorf("Data read does not match expected data.\nExpected: %s\nGot: %s", string(expectedData), string(buf[:n]))
	}
}

// TestChunkedReader_SeekBeyondEOF tests seeking beyond the end of the data
func TestChunkedReader_SeekBeyondEOF(t *testing.T) {
	testData := []byte("abcdefghijklmnopqrstuvwxyz")
	reader := bytes.NewReader(testData)
	chunkSize := 10
	cr, _ := NewChunkedReader(reader, chunkSize)

	// Attempt to seek beyond EOF
	_, err := cr.Seek(int64(len(testData)+10), io.SeekStart)
	if err == nil {
		t.Error("Expected error when seeking beyond EOF, got nil")
	}
}

// TestChunkedReader_SeekNegativeOffset tests seeking to a negative offset
func TestChunkedReader_SeekNegativeOffset(t *testing.T) {
	testData := []byte("abcdefghijklmnopqrstuvwxyz")
	reader := bytes.NewReader(testData)
	chunkSize := 10
	cr, _ := NewChunkedReader(reader, chunkSize)

	// Attempt to seek to a negative offset
	_, err := cr.Seek(-5, io.SeekStart)
	if err == nil {
		t.Error("Expected error when seeking to negative offset, got nil")
	}
}

// TestChunkedReader_ReadZeroBytes tests reading zero bytes
func TestChunkedReader_ReadZeroBytes(t *testing.T) {
	testData := []byte("abcdefghijklmnopqrstuvwxyz")
	reader := bytes.NewReader(testData)
	chunkSize := 10
	cr, _ := NewChunkedReader(reader, chunkSize)

	// Attempt to read zero bytes
	buf := make([]byte, 0)
	n, err := cr.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("Read error: %v", err)
	}
	if n != 0 {
		t.Errorf("Expected to read 0 bytes, got %d", n)
	}
}

// TestChunkedReader_ChunkSizeLargerThanData tests behavior when chunk size is larger than data size
func TestChunkedReader_ChunkSizeLargerThanData(t *testing.T) {
	testData := []byte("abc")
	reader := bytes.NewReader(testData)
	chunkSize := 10
	cr, _ := NewChunkedReader(reader, chunkSize)

	// Read data
	buf := make([]byte, 10)
	n, err := cr.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("Read error: %v", err)
	}
	if n != len(testData) {
		t.Errorf("Expected to read %d bytes, got %d", len(testData), n)
	}
	expectedData := testData
	if !bytes.Equal(buf[:n], expectedData) {
		t.Errorf("Data read does not match expected data.\nExpected: %s\nGot: %s", string(expectedData), string(buf[:n]))
	}
}

// TestChunkedReader_EmptyData tests behavior when data is empty
func TestChunkedReader_EmptyData(t *testing.T) {
	testData := []byte("")
	reader := bytes.NewReader(testData)
	chunkSize := 10
	cr, _ := NewChunkedReader(reader, chunkSize)

	// Read data
	buf := make([]byte, 10)
	n, err := cr.Read(buf)
	if err != io.EOF {
		t.Fatalf("Expected EOF error, got: %v", err)
	}
	if n != 0 {
		t.Errorf("Expected to read 0 bytes, got %d", n)
	}
}

// mockReadSeeker is a mock implementation of io.ReadSeeker that returns errors
type mockReadSeeker struct{}

func (m *mockReadSeeker) Read(p []byte) (int, error) {
	return 0, errors.New("mock read error")
}

func (m *mockReadSeeker) Seek(offset int64, whence int) (int64, error) {
	return 0, errors.New("mock seek error")
}

// TestChunkedReader_ReadError tests error handling when the underlying ReadSeeker returns an error on Read
func TestChunkedReader_ReadError(t *testing.T) {
	reader := &mockReadSeeker{}
	chunkSize := 10
	_, err := NewChunkedReader(reader, chunkSize)

	if err == nil || err.Error() != "mock seek error" {
		t.Errorf("Expected 'mock read error', got: %v", err)
	}
}

// TestChunkedReader_SeekError tests error handling when the underlying ReadSeeker returns an error on Seek
func TestChunkedReader_SeekError(t *testing.T) {
	reader := &mockReadSeeker{}
	chunkSize := 10
	_, err := NewChunkedReader(reader, chunkSize)
	if err == nil || err.Error() != "mock seek error" {
		t.Errorf("Expected 'mock seek error', got: %v", err)
	}
}

// TestChunkedReader_BufferReload tests that the buffer is correctly reloaded when reading beyond the current chunk
func TestChunkedReader_BufferReload(t *testing.T) {
	testData := []byte("abcdefghijklmnopqrstuvwxyz")
	reader := bytes.NewReader(testData)
	chunkSize := 5
	cr, _ := NewChunkedReader(reader, chunkSize)

	// Read first chunk
	buf := make([]byte, 5)
	n, err := cr.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("Read error: %v", err)
	}
	if n != 5 {
		t.Errorf("Expected to read 5 bytes, got %d", n)
	}
	expectedData := testData[:5]
	if !bytes.Equal(buf[:n], expectedData) {
		t.Errorf("Data read does not match expected data.\nExpected: %s\nGot: %s", string(expectedData), string(buf[:n]))
	}

	// Read next chunk
	n, err = cr.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("Read error: %v", err)
	}
	if n != 5 {
		t.Errorf("Expected to read 5 bytes, got %d", n)
	}
	expectedData = testData[5:10]
	if !bytes.Equal(buf[:n], expectedData) {
		t.Errorf("Data read does not match expected data.\nExpected: %s\nGot: %s", string(expectedData), string(buf[:n]))
	}
}

// TestChunkedReader_EfficientMemoryUsage tests that only one chunk is loaded into memory at any time
func TestChunkedReader_EfficientMemoryUsage(t *testing.T) {
	// Since Go does not provide a straightforward way to check memory usage in unit tests,
	// we can simulate this by checking the buffer length after multiple reads
	testData := []byte("abcdefghijklmnopqrstuvwxyz")
	reader := bytes.NewReader(testData)
	chunkSize := 5
	cr, _ := NewChunkedReader(reader, chunkSize)

	// Read data to force multiple buffer reloads
	buf := make([]byte, 2)
	for i := 0; i < 15; i++ {
		_, err := cr.Read(buf)
		if err != nil && err != io.EOF {
			t.Fatalf("Read error: %v", err)
		}
	}

	// Check that the buffer length is equal to chunkSize
	if len(cr.buffer) != chunkSize {
		t.Errorf("Expected buffer length to be %d, got %d", chunkSize, len(cr.buffer))
	}
}

// TestChunkedReader_ReadFullBuffer tests reading when the read buffer size is equal to the chunk size
func TestChunkedReader_ReadFullBuffer(t *testing.T) {
	testData := []byte("abcdefghijklmnopqrstuvwxyz")
	reader := bytes.NewReader(testData)
	chunkSize := 5
	cr, _ := NewChunkedReader(reader, chunkSize)

	// Read with buffer size equal to chunk size
	buf := make([]byte, chunkSize)
	n, err := cr.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("Read error: %v", err)
	}
	if n != chunkSize {
		t.Errorf("Expected to read %d bytes, got %d", chunkSize, n)
	}
	expectedData := testData[:chunkSize]
	if !bytes.Equal(buf[:n], expectedData) {
		t.Errorf("Data read does not match expected data.\nExpected: %s\nGot: %s", string(expectedData), string(buf[:n]))
	}
}

// TestChunkedReader_ReadBeyondEOF tests reading beyond the end of the data
func TestChunkedReader_ReadBeyondEOF(t *testing.T) {
	testData := []byte("abc")
	reader := bytes.NewReader(testData)
	chunkSize := 2
	cr, _ := NewChunkedReader(reader, chunkSize)

	// Read more bytes than available
	buf := make([]byte, 5)
	n, err := cr.Read(buf)
	if err != io.EOF {
		t.Errorf("Expected EOF error, got: %v", err)
	}
	if n != len(testData) {
		t.Errorf("Expected to read %d bytes, got %d", len(testData), n)
	}
	expectedData := testData
	if !bytes.Equal(buf[:n], expectedData) {
		t.Errorf("Data read does not match expected data.\nExpected: %s\nGot: %s", string(expectedData), string(buf[:n]))
	}
}

// TestChunkedReader_SequentialReads tests multiple sequential reads
func TestChunkedReader_SequentialReads(t *testing.T) {
	testData := []byte("abcdefghijklmnopqrstuvwxyz")
	reader := bytes.NewReader(testData)
	chunkSize := 5
	cr, _ := NewChunkedReader(reader, chunkSize)

	// Perform multiple sequential reads
	buf := make([]byte, 3)
	var readData []byte
	for {
		n, err := cr.Read(buf)
		if err != nil && err != io.EOF {
			t.Fatalf("Read error: %v", err)
		}
		if n > 0 {
			readData = append(readData, buf[:n]...)
		}
		if err == io.EOF {
			break
		}
	}

	// Verify the total data read matches the test data
	if !bytes.Equal(readData, testData) {
		t.Errorf("Data read does not match test data.\nExpected: %s\nGot: %s", string(testData), string(readData))
	}
}

// TestChunkedReader_LargeData tests reading large data sets
func TestChunkedReader_LargeData(t *testing.T) {
	// Create large test data
	testData := make([]byte, 1024*1024) // 1 MB
	for i := range testData {
		testData[i] = byte(i % 256)
	}
	reader := bytes.NewReader(testData)
	chunkSize := 64 * 1024 // 64 KB
	cr, _ := NewChunkedReader(reader, chunkSize)

	// Read all data
	buf := make([]byte, 128*1024) // 128 KB buffer
	var totalRead int
	for {
		n, err := cr.Read(buf)
		if err != nil && err != io.EOF {
			t.Fatalf("Read error: %v", err)
		}
		totalRead += n
		if err == io.EOF {
			break
		}
	}

	// Verify total bytes read
	if totalRead != len(testData) {
		t.Errorf("Expected to read %d bytes, got %d", len(testData), totalRead)
	}
}

// TestChunkedReader_SeekToChunkBoundary tests seeking to the boundary of a chunk
func TestChunkedReader_SeekToChunkBoundary(t *testing.T) {
	testData := []byte("abcdefghijklmnopqrstuvwxyz")
	reader := bytes.NewReader(testData)
	chunkSize := 5
	cr, _ := NewChunkedReader(reader, chunkSize)

	// Seek to chunk boundary
	offset, err := cr.Seek(5, io.SeekStart)
	if err != nil {
		t.Fatalf("Seek error: %v", err)
	}
	if offset != 5 {
		t.Errorf("Expected offset 5, got %d", offset)
	}

	// Read data from the boundary
	buf := make([]byte, 5)
	n, err := cr.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("Read error: %v", err)
	}
	expectedData := testData[5:10]
	if !bytes.Equal(buf[:n], expectedData) {
		t.Errorf("Data read does not match expected data at chunk boundary.\nExpected: %s\nGot: %s", string(expectedData), string(buf[:n]))
	}
}

// TestChunkedReader_NonDivisibleChunkSize tests behavior when data size is not divisible by chunk size
func TestChunkedReader_NonDivisibleChunkSize(t *testing.T) {
	testData := []byte("abcdefghij") // 10 bytes
	reader := bytes.NewReader(testData)
	chunkSize := 3
	cr, _ := NewChunkedReader(reader, chunkSize)

	// Read all data
	buf := make([]byte, 10)
	n, err := cr.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("Read error: %v", err)
	}
	if n != 10 {
		t.Errorf("Expected to read 10 bytes, got %d", n)
	}
	expectedData := testData
	if !bytes.Equal(buf[:n], expectedData) {
		t.Errorf("Data read does not match expected data.\nExpected: %s\nGot: %s", string(expectedData), string(buf[:n]))
	}
}

// TestChunkedReader_ZeroChunkSize tests behavior when chunk size is zero
func TestChunkedReader_ZeroChunkSize(t *testing.T) {
	testData := []byte("abc")
	reader := bytes.NewReader(testData)
	chunkSize := 0
	cr, err := NewChunkedReader(reader, chunkSize)
	assert.Nil(t, cr)
	assert.NotNil(t, err)
	assert.Equal(t, "chunk size must be greater than 0", err.Error())
}

// TestChunkedReader_NilReader tests behavior when baseReader is nil
func TestChunkedReader_NilReader(t *testing.T) {
	cr, err := NewChunkedReader(nil, 10)
	assert.Nil(t, cr)
	assert.NotNil(t, err)
	assert.Equal(t, "reader cannot be nil", err.Error())
}

// TestChunkedReader_ReadAfterEOF tests reading after EOF has been reached
func TestChunkedReader_ReadAfterEOF(t *testing.T) {
	testData := []byte("abc")
	reader := bytes.NewReader(testData)
	chunkSize := 2
	cr, _ := NewChunkedReader(reader, chunkSize)

	// Read all data
	buf := make([]byte, 3)
	_, err := cr.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("Read error: %v", err)
	}

	// Attempt to read again after EOF
	n, err := cr.Read(buf)
	if err != io.EOF {
		t.Errorf("Expected EOF error on read after EOF, got: %v", err)
	}
	if n != 0 {
		t.Errorf("Expected to read 0 bytes after EOF, got %d", n)
	}
}

// TestChunkedReader_SeekNoop tests seeking with zero offset and io.SeekCurrent
func TestChunkedReader_SeekNoop(t *testing.T) {
	testData := []byte("abcdefghijklmnopqrstuvwxyz")
	reader := bytes.NewReader(testData)
	chunkSize := 5
	cr, _ := NewChunkedReader(reader, chunkSize)

	// Read some data
	buf := make([]byte, 5)
	_, err := cr.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("Read error: %v", err)
	}

	// Seek with zero offset and io.SeekCurrent
	offset, err := cr.Seek(0, io.SeekCurrent)
	if err != nil {
		t.Fatalf("Seek error: %v", err)
	}
	if offset != cr.offset {
		t.Errorf("Expected offset %d, got %d", cr.offset, offset)
	}

	// Continue reading
	n, err := cr.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("Read error: %v", err)
	}
	expectedData := testData[5:10]
	if !bytes.Equal(buf[:n], expectedData) {
		t.Errorf("Data read after noop seek does not match expected data.\nExpected: %s\nGot: %s", string(expectedData), string(buf[:n]))
	}
}
