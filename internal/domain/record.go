package domain

// RecordMeta represents the metadata of a record.
// The total size of RecordMeta is 64 bytes.
type RecordMeta struct {
	Timestamp     uint64 // 8 bytes - UNIX timestamp
	RecordSize    uint64 // 8 bytes - Size of the entire record (including metadata, labels, and message)
	SchemaVersion uint64 // 8 bytes - Version of the schema
	LabelsSize    uint64 // 8 bytes - Total size of the labels section in bytes
	LabelsCount   uint64 // 8 bytes - Number of labels
	MessageSize   uint64 // 8 bytes - Size of the message in bytes
}

const RecordMetaSize = 64

// Record represents a complete record, including metadata, labels, and message.
type Record struct {
	Meta    RecordMeta
	Labels  []Label
	Message []byte // Variable size - Message content
}
