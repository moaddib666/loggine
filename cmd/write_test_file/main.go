package main

import (
	"LogDb/internal/adapters/serializer"
	"LogDb/internal/domain"
	"LogDb/internal/ports"
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/google/uuid"
	"io"
	"os"
	"time"
)

const fileName = "test.chunk"
const recordInPage = 10
const pagesCount = 10

func main() {

	codec := &serializer.BinarySerializer{}
	fh, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}
	defer fh.Close()

	// Write Header
	_, err = writeDataFileHeader(0, codec, fh)
	if err != nil {
		panic(err)
	}
	for i := 0; i < pagesCount; i++ {
		err = writeDataPage(uint32(i), recordInPage, codec, fh)
		if err != nil {
			panic(err)
		}
	}

	// Update Header
	_, err = fh.Seek(0, io.SeekStart)
	if err != nil {
		panic(err)
	}
	_, err = writeDataFileHeader(uint64(recordInPage*pagesCount), codec, fh)
	if err != nil {
		panic(err)
	}
}

// writeDataFileHeader
func writeDataFileHeader(recordsCount uint64, codec ports.Serializer, writer *os.File) (int, error) {
	ts := time.Now()
	header := &domain.DataFileHeader{
		Version:     0,
		Id:          uuid.New().ID(),
		Year:        uint64(ts.Year()),
		Month:       uint64(ts.Month()),
		Day:         uint64(ts.Day()),
		RecordCount: recordsCount,
		Checksum:    0,
	}
	writer.Seek(0, io.SeekStart)

	return codec.WriteFileHeader(header, writer)
}

// writeDataPage
func writeDataPage(number uint32, recordsCount uint64, codec ports.Serializer, writer *os.File) error {
	buf := &bytes.Buffer{}
	ts := time.Now()
	var pageSize uint64
	for i := 0; i < int(recordsCount); i++ {
		// int64(i) in binary.BigEndian.PutUint64 is a placeholder for the timestamp
		iBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(iBuf, uint64(i))
		record := domain.LogRecord{
			Timestamp:     time.Date(ts.Year(), ts.Month(), ts.Day(), 0, int(number), i, 0, time.Local),
			SchemaVersion: 1,
			Labels: []domain.Label{
				{
					Type:  0,
					Size:  uint64(len("sample_value")),
					Value: []byte("sample_value"),
				},
				{
					Type:  2,
					Size:  8,
					Value: iBuf,
				},
			},
			Message: []byte(fmt.Sprintf("This is just a test row record for page %d and record %d", number, i)),
		}
		n, err := codec.WriteLogRecord(&record, buf)
		if err != nil {
			return err
		}
		pageSize += uint64(n)
	}
	header := &domain.DataPageHeader{
		Number:      number,
		PageSize:    pageSize,
		RecordCount: recordsCount,
	}
	_, err := codec.WriteDataPageHeader(header, writer)
	if err != nil {
		return err
	}
	err = binary.Write(writer, binary.LittleEndian, buf.Bytes())
	if err != nil {
		return err
	}
	return nil
}
