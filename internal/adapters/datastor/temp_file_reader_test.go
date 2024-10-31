package datastor

import (
	"io"
	"io/ioutil"
	"os"
	"runtime"
	"testing"
	"time"
)

// TestTempFileReader_NormalRead tests normal read operations before TTL expiration
func TestTempFileReader_NormalRead(t *testing.T) {
	// Create a temporary file with some content
	tmpFile, err := ioutil.TempFile("", "test_temp_file_reader_*")
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}
	defer os.Remove(tmpFile.Name()) // Clean up in case of test failure

	content := []byte("Hello, World!")
	_, err = tmpFile.Write(content)
	if err != nil {
		tmpFile.Close()
		t.Fatalf("Failed to write to temporary file: %v", err)
	}

	// Seek to the beginning for reading
	_, err = tmpFile.Seek(0, io.SeekStart)
	if err != nil {
		tmpFile.Close()
		t.Fatalf("Failed to seek in temporary file: %v", err)
	}

	// Create tempFileReader with a TTL of 5 seconds
	ttl := 5 * time.Second
	tfr := newTempFileReader(tmpFile, ttl)

	// Read from the tempFileReader
	buf := make([]byte, len(content))
	n, err := tfr.Read(buf)
	if err != nil {
		tfr.Close()
		t.Fatalf("Read error: %v", err)
	}
	if n != len(content) {
		tfr.Close()
		t.Fatalf("Expected to read %d bytes, got %d", len(content), n)
	}
	if string(buf) != string(content) {
		tfr.Close()
		t.Fatalf("Expected content '%s', got '%s'", string(content), string(buf))
	}

	// Close the tempFileReader
	err = tfr.Close()
	if err != nil {
		t.Fatalf("Close error: %v", err)
	}

	// Verify that the file has been deleted
	if _, err := os.Stat(tmpFile.Name()); !os.IsNotExist(err) {
		t.Errorf("Expected temporary file to be deleted")
	}
}

// TestTempFileReader_TTLExpiration tests automatic closure after TTL due to inactivity
func TestTempFileReader_TTLExpiration(t *testing.T) {
	// Create a temporary file with some content
	tmpFile, err := ioutil.TempFile("", "test_temp_file_reader_*")
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}
	defer os.Remove(tmpFile.Name()) // Clean up in case of test failure

	content := []byte("Hello, World!")
	_, err = tmpFile.Write(content)
	if err != nil {
		tmpFile.Close()
		t.Fatalf("Failed to write to temporary file: %v", err)
	}

	// Seek to the beginning for reading
	_, err = tmpFile.Seek(0, io.SeekStart)
	if err != nil {
		tmpFile.Close()
		t.Fatalf("Failed to seek in temporary file: %v", err)
	}

	// Create tempFileReader with a TTL of 2 seconds
	ttl := 2 * time.Second
	tfr := newTempFileReader(tmpFile, ttl)

	// Read part of the content
	buf := make([]byte, 5)
	n, err := tfr.Read(buf)
	if err != nil {
		tfr.Close()
		t.Fatalf("Read error: %v", err)
	}
	if n != 5 {
		tfr.Close()
		t.Fatalf("Expected to read 5 bytes, got %d", n)
	}

	// Wait for TTL to expire
	time.Sleep(ttl + 1*time.Second)

	// Attempt to read again after TTL expiration
	n, err = tfr.Read(buf)
	if err != io.ErrClosedPipe {
		t.Errorf("Expected io.ErrClosedPipe after TTL expiration, got error: %v", err)
	}
	if n != 0 {
		t.Errorf("Expected to read 0 bytes after TTL expiration, got %d", n)
	}

	// Verify that the file has been deleted
	if _, err := os.Stat(tmpFile.Name()); !os.IsNotExist(err) {
		t.Errorf("Expected temporary file to be deleted after TTL expiration")
	}
}

// TestTempFileReader_InactivityResetsTTL tests that activity resets the TTL timer
func TestTempFileReader_InactivityResetsTTL(t *testing.T) {
	// Create a temporary file with some content
	tmpFile, err := ioutil.TempFile("", "test_temp_file_reader_*")
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}
	defer os.Remove(tmpFile.Name()) // Clean up in case of test failure

	content := []byte("Hello, World!")
	_, err = tmpFile.Write(content)
	if err != nil {
		tmpFile.Close()
		t.Fatalf("Failed to write to temporary file: %v", err)
	}

	// Seek to the beginning for reading
	_, err = tmpFile.Seek(0, io.SeekStart)
	if err != nil {
		tmpFile.Close()
		t.Fatalf("Failed to seek in temporary file: %v", err)
	}

	// Create tempFileReader with a TTL of 3 seconds
	ttl := 3 * time.Second
	tfr := newTempFileReader(tmpFile, ttl)

	// Read in intervals less than TTL to reset the timer
	buf := make([]byte, 1)
	for i := 0; i < len(content); i++ {
		n, err := tfr.Read(buf)
		if err != nil {
			tfr.Close()
			t.Fatalf("Read error at iteration %d: %v", i, err)
		}
		if n != 1 {
			tfr.Close()
			t.Fatalf("Expected to read 1 byte, got %d", n)
		}
		// Wait less than TTL
		time.Sleep(1 * time.Second)
	}

	// Wait less than TTL after last read
	time.Sleep(2 * time.Second)

	// Attempt to read again (should fail because we've reached EOF)
	n, err := tfr.Read(buf)
	if err != io.EOF {
		tfr.Close()
		t.Errorf("Expected io.EOF after reading all content, got error: %v", err)
	}

	// Wait for TTL to expire
	time.Sleep(ttl + 1*time.Second)

	// Attempt to read after TTL expiration
	n, err = tfr.Read(buf)
	if err != io.ErrClosedPipe {
		t.Errorf("Expected io.ErrClosedPipe after TTL expiration, got error: %v", err)
	}
	if n != 0 {
		t.Errorf("Expected to read 0 bytes after TTL expiration, got %d", n)
	}

	// Verify that the file has been deleted
	if _, err := os.Stat(tmpFile.Name()); !os.IsNotExist(err) {
		t.Errorf("Expected temporary file to be deleted after TTL expiration")
	}
}

// TestTempFileReader_CloseMethod tests that calling Close closes the file immediately
func TestTempFileReader_CloseMethod(t *testing.T) {
	// Create a temporary file with some content
	tmpFile, err := ioutil.TempFile("", "test_temp_file_reader_*")
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}
	defer os.Remove(tmpFile.Name()) // Clean up in case of test failure

	content := []byte("Hello, World!")
	_, err = tmpFile.Write(content)
	if err != nil {
		tmpFile.Close()
		t.Fatalf("Failed to write to temporary file: %v", err)
	}

	// Seek to the beginning for reading
	_, err = tmpFile.Seek(0, io.SeekStart)
	if err != nil {
		tmpFile.Close()
		t.Fatalf("Failed to seek in temporary file: %v", err)
	}

	// Create tempFileReader with a TTL of 10 seconds
	ttl := 10 * time.Second
	tfr := newTempFileReader(tmpFile, ttl)

	// Close the tempFileReader immediately
	err = tfr.Close()
	if err != nil {
		t.Fatalf("Close error: %v", err)
	}

	// Attempt to read after Close
	buf := make([]byte, 5)
	n, err := tfr.Read(buf)
	if err != io.ErrClosedPipe {
		t.Errorf("Expected io.ErrClosedPipe after Close, got error: %v", err)
	}
	if n != 0 {
		t.Errorf("Expected to read 0 bytes after Close, got %d", n)
	}

	// Verify that the file has been deleted
	if _, err := os.Stat(tmpFile.Name()); !os.IsNotExist(err) {
		t.Errorf("Expected temporary file to be deleted after Close")
	}
}

// TestTempFileReader_GoroutineExit tests that the monitoring goroutine exits after closure
func TestTempFileReader_GoroutineExit(t *testing.T) {
	// Create a temporary file with some content
	tmpFile, err := ioutil.TempFile("", "test_temp_file_reader_*")
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}
	defer os.Remove(tmpFile.Name()) // Clean up in case of test failure

	// Create tempFileReader with a short TTL
	ttl := 1 * time.Second
	tfr := newTempFileReader(tmpFile, ttl)

	// Capture the number of goroutines before
	numGoroutinesBefore := runtime.NumGoroutine()

	// Wait for TTL to expire and the goroutine to exit
	time.Sleep(ttl + 2*time.Second)

	// Capture the number of goroutines after
	numGoroutinesAfter := runtime.NumGoroutine()

	// Allow some time for goroutine scheduling
	time.Sleep(100 * time.Millisecond)

	// Check if the number of goroutines decreased
	if numGoroutinesAfter > numGoroutinesBefore {
		t.Errorf("Expected monitoring goroutine to exit after closure")
	}

	// Clean up
	tfr.Close()
}

// TestTempFileReader_Seek tests the Seek method and TTL updates
func TestTempFileReader_Seek(t *testing.T) {
	// Create a temporary file with some content
	tmpFile, err := ioutil.TempFile("", "test_temp_file_reader_*")
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}
	defer os.Remove(tmpFile.Name()) // Clean up in case of test failure

	content := []byte("Hello, World!")
	_, err = tmpFile.Write(content)
	if err != nil {
		tmpFile.Close()
		t.Fatalf("Failed to write to temporary file: %v", err)
	}

	// Create tempFileReader with a TTL of 3 seconds
	ttl := 3 * time.Second
	tfr := newTempFileReader(tmpFile, ttl)

	// Seek to a position
	_, err = tfr.Seek(7, io.SeekStart)
	if err != nil {
		tfr.Close()
		t.Fatalf("Seek error: %v", err)
	}

	// Read from the new position
	buf := make([]byte, 5)
	n, err := tfr.Read(buf)
	if err != nil {
		tfr.Close()
		t.Fatalf("Read error after Seek: %v", err)
	}
	if n != 5 {
		tfr.Close()
		t.Fatalf("Expected to read 5 bytes, got %d", n)
	}
	if string(buf) != "World" {
		tfr.Close()
		t.Fatalf("Expected to read 'World', got '%s'", string(buf))
	}

	// Wait for TTL to expire
	time.Sleep(ttl + 1*time.Second)

	// Attempt to read after TTL expiration
	n, err = tfr.Read(buf)
	if err != io.ErrClosedPipe {
		t.Errorf("Expected io.ErrClosedPipe after TTL expiration, got error: %v", err)
	}

	// Verify that the file has been deleted
	if _, err := os.Stat(tmpFile.Name()); !os.IsNotExist(err) {
		t.Errorf("Expected temporary file to be deleted after TTL expiration")
	}
}
