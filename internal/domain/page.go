package domain

type DataPageHeader struct {
	Number      uint32 // 4 bytes - Minute number in 24 hours (0-1439)
	PageSize    uint64 // 8 bytes - Size of the page in bytes
	RecordCount uint64 // 8 bytes - Number of records in the page
}

const DataPageHeaderSize = 20

type DataPage struct {
	Header  DataPageHeader
	Records []Record
}
