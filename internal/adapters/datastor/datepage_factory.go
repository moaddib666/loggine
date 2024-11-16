package datastor

import (
	"LogDb/internal/domain"
	"LogDb/internal/ports"
)

var _ ports.DataPageHeaderFactory = &DataPageHeaderFactory{}

type DataPageHeaderFactory struct {
}

func (d *DataPageHeaderFactory) NewEmptyPageHeader() *domain.DataPageHeader {
	return domain.NewEmptyDataPageHeader()
}

func (d *DataPageHeaderFactory) FromLogRecord(record *domain.LogRecord) *domain.DataPageHeader {
	return d.FromMinuteNumber(uint32(record.Timestamp.Minute() + record.Timestamp.Hour()*60))
}

func (d *DataPageHeaderFactory) FromMinuteNumber(number uint32) *domain.DataPageHeader {
	return domain.NewDataPageHeaderForMinute(number)
}

// NewDataPageHeaderFactory
func NewDataPageHeaderFactory() *DataPageHeaderFactory {
	return &DataPageHeaderFactory{}
}
