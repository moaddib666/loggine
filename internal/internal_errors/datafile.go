package internal_errors

import "errors"

var DataPageNumberOutOfRange = errors.New("DataPageNumberOutOfRange")
var DataPageDoesNotExist = errors.New("DataPageDoesNotExist")
var AttemptToWriteToDataInPast = errors.New("AttemptToWriteToDataInPast")
var NoDataPagesLeft = errors.New("NoDataPagesLeft")
var DataPageNotSelected = errors.New("DataPageNotSelected")
var DataPageNumberMismatch = errors.New("DataPageNumberMismatch")
var DataFileNumberMismatch = errors.New("DataFileNumberMismatch")
var DataPageRecordSizeMismatch = errors.New("DataPageRecordSizeMismatch")
var DataFileAlreadyCompressed = errors.New("DataFileAlreadyCompressed")
