package domain

import (
	"time"
)

type LogChunk struct {
	Records []LogRecord
}

type LogRecord struct {
	Timestamp     time.Time
	SchemaVersion uint64
	Labels        []Label
	Message       []byte
}

// NewEmptyLogRecord creates a new LogRecord with the current time
func NewEmptyLogRecord() LogRecord {
	return LogRecord{Timestamp: time.Now(), SchemaVersion: 1, Labels: []Label{}, Message: []byte{}}
}

// AddLabel a new label to the record
func (r *LogRecord) AddLabel(label Label) {
	r.Labels = append(r.Labels, label)
}

//func () {
//	for _, label := range record.Labels {
//		switch label.Type {
//		case 0: // String
//			labelValue := string(label.Value) // Convert bytes back to string
//			fmt.Println("String Label:", labelValue)
//
//		case 1: // Integer
//			labelValue := binary.LittleEndian.Uint64(label.Value) // Convert bytes back to int64
//			fmt.Println("Integer Label:", labelValue)
//
//		case 2: // Float
//			labelValue := math.Float64frombits(binary.LittleEndian.Uint64(label.Value)) // Convert bytes back to float64
//			fmt.Println("Float Label:", labelValue)
//
//		default:
//			fmt.Println("Unknown label type")
//		}
//	}
//}
