package ports

import "io"

type LogRecordLabelsWriter interface {
	WriteLogRecordLabels(labels map[string]string, writer io.Writer) (int, error)
}

type LogRecordLabelsReader interface {
	ReadLogRecordLabels(reader io.Reader) (map[string]string, int, error)
}

type LogRecordMessageWriter interface {
	WriteLogRecordMessage(message string) (int, error)
}

type LogRecordMessageReader interface {
	ReadLogRecordMessage() (string, int, error)
}
