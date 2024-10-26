# Write Ahead Logging

The WAL allows to create buffer incoming data and write it to the disk in a sequential way.
The WAL is used to avoid the random write to the disk and improve the write performance.
The WAL is used to write the data pages and the index pages.

## WAL Structure

### Name

- {uuid}.wal
- {uuid}.wal.dirty

### WAL Size

- max size: 100 MB
- max records: 1_000_000

### Header

The header contains the following fields:

- Id: 4 bytes - uint32 - uuid
- Version: 1 byte - uint8
- CreatedAt: 8 bytes - uint64 - unix timestamp
- ReadCursor: 8 bytes - uint64 - read cursor
- LogRecords: []LogRecord - list of log records

### Flush

The WAL is flushed when the max size or max records is reached or by timer default 5s.

### Process

The WAL must be automatically discovered and processed by storage nodes.
On application startup scan for dirty *.wal.dirty and *.wal files and process them.
If dirty files are found die and let the operator fix the issue.
Process the wal files in order of {uuid} oldest to newest.
Process include sorting the log records by timestamp and writing the data pages and index pages.

- Read the WAL file
- Read Header
- loop over LogRecords
    - read log metadata
    - understand the DataFile and DataPage
        - create DataFile if not exists
        - open DataFile and read Header
        - seek to Last DataPage
        - If DataPage is not fit for log record
            - create new DataPage
            - write log record to DataPage
            - update DataFile Header
        - create DataPage if not exists
    - write log record to DataPage
    - update ReadCursor
    - update Index


