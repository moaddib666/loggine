package index_test

import (
	"LogDb/internal/adapters/index"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestSimultaneousReadOperations(t *testing.T) {
	idx := &index.PrimaryIndexItem{}
	// Simulate multiple read operations
	op1, err := idx.RequestReadAccess()
	require.NoErrorf(t, err, "Read operation 1 should have succeeded")
	op2, err := idx.RequestReadAccess()
	require.NoErrorf(t, err, "Read operation 2 should have succeeded")
	err = op1.Done()
	require.NoErrorf(t, err, "Read operation 1 should have succeeded")
	err = op2.Done()
	require.NoErrorf(t, err, "Read operation 2 should have succeeded")
}

func TestBlockedWriteDuringReads(t *testing.T) {
	idx := &index.PrimaryIndexItem{}

	// Simulate a read operation
	op1, err := idx.RequestReadAccess()
	require.NoErrorf(t, err, "Read operation 1 should have succeeded")

	// Simulate a write operation
	op2, err := idx.RequestWriteAccess()
	require.Errorf(t, err, "Write operation should have failed")
	require.Nil(t, op2, "Write operation should not have been created")

	err = op1.Done()
	require.NoErrorf(t, err, "Read operation 1 should have succeeded")

	// Simulate a write operation
	op3, err := idx.RequestWriteAccess()
	require.NoErrorf(t, err, "Write operation should have succeeded")
	err = op3.Done()
	require.NoErrorf(t, err, "Write operation should have succeeded")
}

func TestBlockedReadsDuringWrite(t *testing.T) {
	idx := &index.PrimaryIndexItem{}

	// Simulate a write operation
	writeOp, err := idx.RequestWriteAccess()
	require.NoErrorf(t, err, "Write operation should have succeeded")

	// Simulate a read operation
	readOp, err := idx.RequestReadAccess()
	require.Errorf(t, err, "Read operation should have failed")
	require.Nil(t, readOp, "Read operation should not have been created")

	err = writeOp.Done()
	require.NoErrorf(t, err, "Write operation should have succeeded")

	// Retry read operation after write is done
	readOp2, err := idx.RequestReadAccess()
	require.NoErrorf(t, err, "Read operation should have succeeded")
	err = readOp2.Done()
	require.NoErrorf(t, err, "Read operation should have succeeded")
}

func TestAwaitReadAccessDuringWrite(t *testing.T) {
	idx := &index.PrimaryIndexItem{}

	// Simulate a write operation
	writeOp, err := idx.RequestWriteAccess()
	require.NoErrorf(t, err, "Write operation should have succeeded")

	// Simulate awaiting read access during the write operation
	readOpChan := make(chan error)
	go func() {
		readOp, err := idx.AwaitReadAccess()
		if err == nil {
			err = readOp.Done()
		}
		readOpChan <- err
	}()

	// Ensure the read operation is blocked while the write is active
	select {
	case <-readOpChan:
		t.Error("Read operation should not have succeeded while write is active")
	case <-time.After(100 * time.Millisecond):
		t.Log("Read operation correctly blocked")
	}

	// Complete the write operation
	err = writeOp.Done()
	require.NoErrorf(t, err, "Write operation should have succeeded")

	// Ensure the read operation proceeds after the write completes
	select {
	case err := <-readOpChan:
		require.NoErrorf(t, err, "Read operation should have succeeded after write completed")
	case <-time.After(100 * time.Millisecond):
		t.Error("Read operation did not proceed after write completed")
	}
}

func TestAwaitWriteAccessDuringReads(t *testing.T) {
	idx := &index.PrimaryIndexItem{}

	// Simulate multiple read operations
	readOp1, err := idx.RequestReadAccess()
	require.NoErrorf(t, err, "Read operation 1 should have succeeded")
	readOp2, err := idx.RequestReadAccess()
	require.NoErrorf(t, err, "Read operation 2 should have succeeded")

	// Simulate awaiting write access during active reads
	writeOpChan := make(chan error)
	go func() {
		writeOp, err := idx.AwaitWriteAccess()
		if err == nil {
			err = writeOp.Done()
		}
		writeOpChan <- err
	}()

	// Ensure the write operation is blocked while reads are active
	select {
	case <-writeOpChan:
		t.Error("Write operation should not have succeeded while reads are active")
	case <-time.After(100 * time.Millisecond):
		t.Log("Write operation correctly blocked")
	}

	// Complete the read operations
	err = readOp1.Done()
	require.NoErrorf(t, err, "Read operation 1 should have succeeded")
	err = readOp2.Done()
	require.NoErrorf(t, err, "Read operation 2 should have succeeded")

	// Ensure the write operation proceeds after the reads complete
	select {
	case err := <-writeOpChan:
		require.NoErrorf(t, err, "Write operation should have succeeded after reads completed")
	case <-time.After(100 * time.Millisecond):
		t.Error("Write operation did not proceed after reads completed")
	}
}
