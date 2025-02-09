package merge

import (
	"LogDb/internal/adapters/datastor"
	"LogDb/internal/domain"
	"LogDb/internal/internal_errors"
	"LogDb/internal/ports"
	ioutils "LogDb/pkg/utils/io"
	"bytes"
	"errors"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"io"
	"time"
)

var _ ports.Merger = &Merger{}

// Merger merges two data pages or data files into one.
type Merger struct {
	repo            ports.DataFileRepository
	dfWriterFactory ports.DataFileWriterFactory
	dfReaderFactory ports.DataFileReaderFactory
	dpReaderFactory ports.DataPageReaderFactory
	codec           ports.Serializer
}

func (m *Merger) MergeDataPages(dp1, dp2 *domain.ReadOnlyDataPage) (*domain.DataPage, error) {
	if dp1.Header.Number != dp2.Header.Number {
		return nil, internal_errors.DataPageNumberMismatch
	}
	dataPageMergedHeader := domain.NewDataPageHeaderForMinute(dp1.Header.Number)
	dataPageMergedHeader.RecordCount = dp1.Header.RecordCount + dp2.Header.RecordCount
	dataPageMergedHeader.PageSize = dp1.Header.PageSize + dp2.Header.PageSize

	heap := make(domain.LogHeap, 0, dp1.Header.RecordCount+dp2.Header.RecordCount)
	reader1 := m.dpReaderFactory.NewDataPageReader(dp1.Header, dp1.ReadSeeker)
	reader2 := m.dpReaderFactory.NewDataPageReader(dp2.Header, dp2.ReadSeeker)
	for i := 0; i < int(dp1.Header.RecordCount); i++ {
		if !reader1.Scan() {
			break
		}
		if record, err := reader1.Record(); err == nil {
			heap.Push(record)
		}
	}
	for i := 0; i < int(dp2.Header.RecordCount); i++ {
		if !reader2.Scan() {
			break
		}
		if record, err := reader2.Record(); err == nil {
			heap.Push(record)
		}
	}

	buffer := bytes.NewBuffer(make([]byte, 0, dataPageMergedHeader.PageSize+uint64(domain.DataPageHeaderSize)))
	_, _ = m.codec.WriteDataPageHeader(dataPageMergedHeader, buffer)
	bufferSize := 0
	for heap.Len() > 0 {
		record := heap.Pop().(*domain.LogRecord)
		if n, err := m.codec.WriteLogRecord(record, buffer); err != nil {
			return nil, err
		} else {
			bufferSize += n
		}
	}
	if bufferSize != int(dataPageMergedHeader.PageSize) {
		return nil, internal_errors.DataPageRecordSizeMismatch
	}
	descriptor := ioutils.NewReadWriteSeeker(buffer)
	return domain.NewDataPage(dataPageMergedHeader, descriptor), nil
}

func (m *Merger) MergeDataFiles(df1, df2 *domain.DataFile) (*domain.DataFile, error) {
	if df1.Header.Time() != df2.Header.Time() {
		return nil, internal_errors.DataFileNumberMismatch
	}
	if df1.Header.FirstDataPageNumber > df2.Header.LastDataPageNumber {
		log.Debugf("Merging by appending %s to %s", df2.Header, df1.Header)
		return m.safeAppendDataFile(df2, df1)
	} else if df2.Header.FirstDataPageNumber > df1.Header.LastDataPageNumber {
		log.Debugf("Merging by appending %s to %s", df1.Header, df2.Header)
		return m.safeAppendDataFile(df1, df2)
	} else if df1.Header.FirstDataPageNumber == df2.Header.LastDataPageNumber {
		log.Debugf("Merging by appending with last page merged %s to %s", df2.Header, df1.Header)
		return m.safeAppendDataFileWithLastPageMerged(df2, df1)
	} else if df2.Header.FirstDataPageNumber == df1.Header.LastDataPageNumber {
		log.Debugf("Merging by appending with last page merged %s to %s", df1.Header, df2.Header)
		return m.safeAppendDataFileWithLastPageMerged(df1, df2)
	} else {
		log.Debugf("Merging by creating a new data file %s and %s", df1.Header, df2.Header)
		return m.mergeToANewDataFile(df1, df2)
	}
}

// safeAppendDataFileWithLastPageMerged appends the data file to the target file.
func (m *Merger) safeAppendDataFileWithLastPageMerged(target, source *domain.DataFile) (*domain.DataFile, error) {
	// Read the last data page from target and first data page from source
	targetReader := m.dfReaderFactory.FromDataFile(target)
	sourceReader := m.dfReaderFactory.FromDataFile(source)

	if err := targetReader.SelectDataPage(target.Header.LastDataPageNumber); err != nil {
		return nil, err
	}
	if err := sourceReader.SelectDataPage(source.Header.FirstDataPageNumber); err != nil {
		return nil, err
	}

	dph1, err := targetReader.GetCurrentDataPageHeader()
	if err != nil {
		return nil, err
	}
	dph2, err := sourceReader.GetCurrentDataPageHeader()
	if err != nil {
		return nil, err
	}

	dp1 := domain.NewReadOnlyDataPage(dph1, targetReader.GetDataPageReader())
	dp2 := domain.NewReadOnlyDataPage(dph2, sourceReader.GetDataPageReader())

	// Merge the data pages
	mergedPage, err := m.MergeDataPages(dp1, dp2)
	if err != nil {
		return nil, err
	}

	// Seek start of file to write the merged data page
	if _, err := target.File.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	// Write the target data file header
	newHeader := m.mergeDataFileHeaders(target, source, target.Header.Id)

	// Write the new header to the target file
	if _, err := m.codec.WriteFileHeader(newHeader, target); err != nil {
		return nil, err
	}
	// Seek to the last data page start of the last file
	if _, err := target.File.Seek(-int64(dph2.PageSize+uint64(domain.DataPageHeaderSize)), io.SeekEnd); err != nil {
		return nil, err
	}
	// Write the merged data page
	if _, err := io.Copy(target.File, mergedPage); err != nil {
		return nil, err
	}
	// Seek df1 to the end of the first data page
	if _, err := source.File.Seek(int64(domain.DataFileHeaderSize+domain.DataPageHeaderSize+int(dph2.PageSize)), io.SeekStart); err != nil {
		return nil, err
	}
	return m.unsafeAppendDataFile(target, source)
}

// safeAppendDataFile appends the data file to the target file.
func (m *Merger) safeAppendDataFile(target,
	source *domain.DataFile) (*domain.DataFile, error) {

	// seek to header end of source file
	if _, err := source.File.Seek(int64(domain.DataFileHeaderSize), io.SeekStart); err != nil {
		return nil, err
	}
	// seek 0 of target file
	if _, err := target.File.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	// Write new merged header
	newHeader := m.mergeDataFileHeaders(target, source, target.Header.Id)
	if _, err := m.codec.WriteFileHeader(newHeader, target); err != nil {
		return nil, err
	}
	// seek end of target file
	if _, err := target.File.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}
	return m.unsafeAppendDataFile(target, source)
}

// unsafeAppendDataFile appends the data file to the target file.
func (m *Merger) unsafeAppendDataFile(target,
	source *domain.DataFile) (*domain.DataFile, error) {
	target.Header.LastDataPageNumber = source.Header.LastDataPageNumber
	target.Header.RecordCount += source.Header.RecordCount
	target.Header.UpdateChecksum()

	if _, err := io.Copy(target.File, source); err != nil {
		return nil, err
	}
	return target, nil
}

// mergeDataFileHeaders merges two data file headers into a new data file header.
func (m *Merger) mergeDataFileHeaders(df1, df2 *domain.DataFile, id uint32) *domain.DataFileHeader {
	header := &domain.DataFileHeader{
		Version:             df1.Header.Version,
		Id:                  id,
		RecordCount:         df1.Header.RecordCount + df2.Header.RecordCount,
		Year:                df1.Header.Year,
		Month:               df1.Header.Month,
		Day:                 df1.Header.Day,
		LastDataPageNumber:  max(df1.Header.LastDataPageNumber, df2.Header.LastDataPageNumber),
		FirstDataPageNumber: min(df1.Header.FirstDataPageNumber, df2.Header.FirstDataPageNumber),
	}
	header.UpdateChecksum()
	return header
}

// mergeToANewDataFile merges two data files into a new data file.
func (m *Merger) mergeToANewDataFile(df1, df2 *domain.DataFile) (*domain.DataFile, error) {
	// Create a new data file header that combines df1 and df2
	newHeader := m.mergeDataFileHeaders(df1, df2, uuid.New().ID())

	// Create a new data file
	mergedDataFile, err := m.repo.CreateFromHeader(newHeader)
	if err != nil {
		return nil, err
	}
	// Write the header to the new data file
	if _, err := m.codec.WriteFileHeader(newHeader, mergedDataFile); err != nil {
		return nil, err
	}

	newDataFileWriter, _ := m.dfWriterFactory.FromDataFile(mergedDataFile)
	// TODO: Work with this looks like an issue
	datastor.WithAutoFlush(5*time.Second, newDataFileWriter)
	// Create a new data file writer
	defer newDataFileWriter.Close()

	// Create data file readers for df1 and df2
	dfReader1 := m.dfReaderFactory.FromDataFile(df1)
	dfReader2 := m.dfReaderFactory.FromDataFile(df2)
	var dfReaderEof1, dfReaderEof2 = false, false
	for {
		if dfReaderEof1 && dfReaderEof2 {
			break
		}
		// Select the data page from df1
		dfReader1CurrentDataPageHeader, err := dfReader1.GetCurrentDataPageHeader()
		if err != nil {
			return nil, err
		}
		dfReader2CurrentDataPageHeader, err := dfReader2.GetCurrentDataPageHeader()
		if err != nil {
			return nil, err
		}

		if dfReader1CurrentDataPageHeader.Number == dfReader2CurrentDataPageHeader.Number {
			dp1 := domain.NewReadOnlyDataPage(dfReader1CurrentDataPageHeader, dfReader1.GetDataPageReader())
			dp2 := domain.NewReadOnlyDataPage(dfReader2CurrentDataPageHeader, dfReader2.GetDataPageReader())

			mergedDataPage, err := m.MergeDataPages(dp1, dp2)
			if err != nil {
				return nil, err
			}

			if _, err := io.Copy(mergedDataFile, mergedDataPage); err != nil {
				return nil, err
			}
			if _, err := dfReader1.NextDataPage(); err != nil {
				if !errors.Is(err, internal_errors.NoDataPagesLeft) {
					return nil, err
				}
				dfReaderEof1 = true
			}
			if _, err := dfReader2.NextDataPage(); err != nil {
				if !errors.Is(err, internal_errors.NoDataPagesLeft) {
					return nil, err
				}
				dfReaderEof2 = true
			}
			continue
		}

		var selectedDataPageHeader *domain.DataPageHeader
		var selectedDataFileReader ports.DataFileReader
		var eofPointer *bool = nil
		// TODO: optimize this if eof1 or eof2 we can just copy the rest of the file to the merged file and break
		if dfReaderEof1 {
			selectedDataPageHeader = dfReader2CurrentDataPageHeader
			selectedDataFileReader = dfReader2
			eofPointer = &dfReaderEof2
		} else if dfReaderEof2 {
			selectedDataPageHeader = dfReader1CurrentDataPageHeader
			selectedDataFileReader = dfReader1
			eofPointer = &dfReaderEof1
		} else if dfReader1CurrentDataPageHeader.Number < dfReader2CurrentDataPageHeader.Number {
			selectedDataPageHeader = dfReader1CurrentDataPageHeader
			selectedDataFileReader = dfReader1
			eofPointer = &dfReaderEof1
		} else {
			selectedDataPageHeader = dfReader2CurrentDataPageHeader
			selectedDataFileReader = dfReader2
			eofPointer = &dfReaderEof2
		}

		if _, err := m.codec.WriteDataPageHeader(selectedDataPageHeader, mergedDataFile); err != nil {
			return nil, err
		}

		if _, err := io.Copy(mergedDataFile, selectedDataFileReader.GetDataPageReader()); err != nil {
			return nil, err
		}
		if _, err := selectedDataFileReader.NextDataPage(); err != nil {
			if !errors.Is(err, internal_errors.NoDataPagesLeft) {
				return nil, err
			}
			*eofPointer = true
		}
	}

	return mergedDataFile, nil
}

// NewMerger creates a new Merger.
func NewMerger(dfWriterFactory ports.DataFileWriterFactory, dfReaderFactory ports.DataFileReaderFactory, dpReaderFactory ports.DataPageReaderFactory, repo ports.DataFileRepository) *Merger {
	return &Merger{
		dfWriterFactory: dfWriterFactory,
		dfReaderFactory: dfReaderFactory,
		dpReaderFactory: dpReaderFactory,
		codec:           repo.Codec(),
		repo:            repo,
	}
}
