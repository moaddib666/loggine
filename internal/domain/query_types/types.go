package query_types

// Operation types
type Operation string

const (
	Select Operation = "select"
	Scan   Operation = "scan"
)

// Aggregation Dimensions
type Dimension string

const (
	Minute  Dimension = "minute"
	Hour    Dimension = "hour"
	Day     Dimension = "day"
	Week    Dimension = "week"
	Month   Dimension = "month"
	Quarter Dimension = "quarter"
	Year    Dimension = "year"
)

// Format options for the result
type Format string

const (
	Text   Format = "text"
	CSV    Format = "csv"
	JSON   Format = "json"
	YAML   Format = "yaml"
	Binary Format = "binary"
)

// QueryOperator for conditions
type QueryOperator string

const (
	Equal        QueryOperator = "="
	GreaterThan  QueryOperator = ">"
	LessThan     QueryOperator = "<"
	GreaterEqual QueryOperator = ">="
	LessEqual    QueryOperator = "<="
	NotEqual     QueryOperator = "!="
	Exists       QueryOperator = "exists"
	IsNull       QueryOperator = "is null"
	IsNotNull    QueryOperator = "is not null"
	And          QueryOperator = "and"
	Or           QueryOperator = "or"
)

// Condition represents a single condition in the where clause
type Condition struct {
	Field    string
	Operator QueryOperator
	Value    interface{}
}
