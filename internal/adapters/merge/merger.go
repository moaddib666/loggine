package merge

import (
	"LogDb/internal/domain"
	"LogDb/internal/internal_errors"
	"LogDb/internal/ports"
	ioutils "LogDb/pkg/utils/io"
	"bytes"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"io"
	"path"
	"path/filepath"
)

var _ ports.Merger = &Merger{}

// Merger merges two data pages or data files into one.
type Merger struct {
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
		return m.safeAppendDataFile(df1, df2)
	} else if df2.Header.FirstDataPageNumber > df1.Header.LastDataPageNumber {
		log.Debugf("Merging by appending %s to %s", df1.Header, df2.Header)
		return m.safeAppendDataFile(df2, df1)
	} else if df1.Header.FirstDataPageNumber == df2.Header.LastDataPageNumber {
		log.Debugf("Merging by appending with last page merged %s to %s", df2.Header, df1.Header)
		return m.safeAppendDataFileWithLastPageMerged(df1, df2)
	} else if df2.Header.FirstDataPageNumber == df1.Header.LastDataPageNumber {
		log.Debugf("Merging by appending with last page merged %s to %s", df1.Header, df2.Header)
		return m.safeAppendDataFileWithLastPageMerged(df2, df1)
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

	// Seek to the last data page start of the last file
	if _, err := target.File.Seek(-int64(dph2.PageSize+uint64(domain.DataPageHeaderSize)), io.SeekEnd); err != nil {
		return nil, err
	}
	// Write the merged data page
	if _, err := io.Copy(target.File, mergedPage); err != nil {
		return nil, err
	}
	// Seek df1 to the end of the first data page
	if _, err := source.File.Seek(int64(domain.DataFileHeaderSize+domain.DataPageHeaderSize+int(dph1.PageSize)), io.SeekStart); err != nil {
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

// mergeToANewDataFile merges two data files into a new data file.
func (m *Merger) mergeToANewDataFile(df1, df2 *domain.DataFile) (*domain.DataFile, error) {
	// Create a new data file header that combines df1 and df2
	newHeader := &domain.DataFileHeader{
		Version:             df1.Header.Version,
		Id:                  uuid.New().ID(),
		RecordCount:         df1.Header.RecordCount + df2.Header.RecordCount,
		Year:                df1.Header.Year,
		Month:               df1.Header.Month,
		Day:                 df1.Header.Day,
		LastDataPageNumber:  max(df1.Header.LastDataPageNumber, df2.Header.LastDataPageNumber),
		FirstDataPageNumber: min(df1.Header.FirstDataPageNumber, df2.Header.FirstDataPageNumber),
	}
	newHeader.UpdateChecksum()

	// FIXME: DataFileRepository should be responsible for creating a new data file
	dirOfDf1 := filepath.Dir(df1.File.Name())
	// Create a new data file
	mergedDataFile, err := domain.NewWriteOnlyDataFile(newHeader, path.Join(dirOfDf1, newHeader.String()))
	if err != nil {
		return nil, err
	}
	// Write the header to the new data file
	if _, err := m.codec.WriteFileHeader(newHeader, mergedDataFile); err != nil {
		return nil, err
	}

	newDataFileWriter, _ := m.dfWriterFactory.FromDataFile(mergedDataFile)

	// Create a new data file writer
	defer newDataFileWriter.Close()

	// Create data file readers for df1 and df2
	dfReader1 := m.dfReaderFactory.FromDataFile(df1)
	dfReader2 := m.dfReaderFactory.FromDataFile(df2)

	for {
		// Select the data page from df1
		dfReader1CurrentDataPageHeader, err := dfReader1.GetCurrentDataPageHeader()
		if err != nil {
			return nil, err
		}
		dfReader2CurrentDataPageHeader, err := dfReader2.GetCurrentDataPageHeader()
		if err != nil {
			return nil, err
		}

		// if page1.Number == page2.Number then merge the pages
		// if page1.Number < page2.Number then write page1 and select next page from df1
		// if page1.Number > page2.Number then write page2 and select next page from df2
		// repeat until both readers are at the end of the file
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
				return nil, err
			}
			if _, err := dfReader2.NextDataPage(); err != nil {
				return nil, err
			}
		} else if dfReader1CurrentDataPageHeader.Number < dfReader2CurrentDataPageHeader.Number {
			// workaround to avoid seek for header
			_, _ = m.codec.WriteDataPageHeader(dfReader1CurrentDataPageHeader, mergedDataFile)
			if _, err := io.Copy(mergedDataFile, dfReader1.GetDataPageReader()); err != nil {
				return nil, err
			}
			if _, err := dfReader1.NextDataPage(); err != nil {
				return nil, err
			}
		} else {
			// workaround to avoid seek for header
			_, _ = m.codec.WriteDataPageHeader(dfReader2CurrentDataPageHeader, mergedDataFile)
			if _, err := io.Copy(mergedDataFile, dfReader2.GetDataPageReader()); err != nil {
				return nil, err
			}
			if _, err := dfReader2.NextDataPage(); err != nil {
				return nil, err
			}
		}
		if mergedDataFile.Header.LastDataPageNumber == dfReader1CurrentDataPageHeader.Number ||
			mergedDataFile.Header.LastDataPageNumber == dfReader2CurrentDataPageHeader.Number {
			break
		}

	}
	return mergedDataFile, nil
}

// NewMerger creates a new Merger.
func NewMerger(dfWriterFactory ports.DataFileWriterFactory, dfReaderFactory ports.DataFileReaderFactory, dpReaderFactory ports.DataPageReaderFactory, codec ports.Serializer) *Merger {
	return &Merger{
		dfWriterFactory: dfWriterFactory,
		dfReaderFactory: dfReaderFactory,
		dpReaderFactory: dpReaderFactory,
		codec:           codec,
	}
}
