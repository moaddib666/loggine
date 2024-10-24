package internal_errors

import (
	"errors"
)

// RecordsLimitReached is an error that is returned when the limit of records has been reached.
var RecordsLimitReached = errors.New("RecordsLimitReached")

// RecordsOutOfRange is an error that is returned when the limit of records has been reached.
var RecordsOutOfRange = errors.New("RecordsOutOfRange")

// PageEndReached is an error that is returned when the limit of records has been reached.
var PageEndReached = errors.New("PageEndReached")
