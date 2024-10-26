package main

import (
	"LogDb/internal/adapters/presenters"
	"LogDb/internal/adapters/serializer"
	"LogDb/internal/domain"
	"LogDb/internal/ports"
	"fmt"
	"io"
	"os"
	"time"
)

const fileName = "test.chunk"

func main() {

	codec := &serializer.BinarySerializer{}
	presenter := presenters.NewLogRecordRawStringPresenter()
	fh, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}
	defer fh.Close()

	// Read Header
	header, err := readDataFileHeader(codec, fh)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Header: %v\n", header)
	for {
		dataPageHeader, dataPageReader, err := readDataPage(codec, fh)
		if err != nil {
			break
		}
		fmt.Printf("Data Page: %v\n", dataPageHeader)
		for {
			record, err := readRecord(codec, dataPageReader)
			if err != nil {
				break
			}
			fmt.Printf(presenter.Present(record))
		}
	}
}

// readDataFileHeader
func readDataFileHeader(codec ports.Serializer, reader *os.File) (*domain.DataFileHeader, error) {
	header := &domain.DataFileHeader{}
	_, err := reader.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}
	_, err = codec.ReadFileHeader(header, reader)
	if err != nil {
		return nil, err
	}
	return header, nil
}

// readDataPage
func readDataPage(codec ports.Serializer, reader *os.File) (*domain.DataPageHeader, io.Reader, error) {
	header := &domain.DataPageHeader{}
	_, err := codec.ReadDataPageHeader(header, reader)
	if err != nil {
		return nil, nil, err
	}
	return header, io.LimitReader(reader, int64(header.PageSize)), nil
}

// readRecord
func readRecord(codec ports.Serializer, reader io.Reader) (*domain.LogRecord, error) {
	record := &domain.LogRecord{}
	recordMeta := &domain.RecordMeta{}
	_, err := codec.ReadLogRecordMeta(recordMeta, reader)
	if err != nil {
		return nil, err
	}
	for i := 0; i < int(recordMeta.LabelsCount); i++ {
		label := domain.Label{}
		_, err = codec.ReadLogLabel(&label, reader)
		if err != nil {
			return nil, err
		}
		record.Labels = append(record.Labels, label)
	}
	message := make([]byte, recordMeta.MessageSize)
	_, err = codec.ReadLogRecordMessage(message, reader)
	if err != nil {
		return nil, err
	}
	record.Timestamp = time.Unix(int64(recordMeta.Timestamp), 0)
	record.Message = message
	return record, nil
}
