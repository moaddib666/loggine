package internal_errors

import "errors"

var DataPageNumberOutOfRange = errors.New("DataPageNumberOutOfRange")
var DataPageDoesNotExist = errors.New("DataPageDoesNotExist")
var AttemptToWriteToDataInPast = errors.New("AttemptToWriteToDataInPast")
var NoDataPagesLeft = errors.New("NoDataPagesLeft")
var DataPageNotSelected = errors.New("DataPageNotSelected")
