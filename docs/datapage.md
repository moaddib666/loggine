# Data page format
The Data page is a file that contains the data of the database. 
It is a binary file that contains the data of the database.
The data page is divided into two parts, the header and the data. 
The header contains the metadata of the page, and the data contains the records of the page.



## Header
uint64 - 8 bytes - Version
uint64 - 8 bytes - Page number
uint64 - 8 bytes - Size
uint64 - 8 bytes - Min Timestamp UNIX
uint64 - 8 bytes - Max Timestamp UNIX
reserved - 256 bytes - Reserved
uint64 - 8 bytes - Checksum

## Data
### Record
#### Meta
uint64 - 8 bytes - Timestamp
uint64 - 8 bytes - Schema Version
uint64 - 8 bytes - Labels length
uint64 - 8 bytes - Labels count
#### Labels
uint64 - 8 bytes - Label value length
uint64 - 8 bytes - label type (0 - string, 1 - int, 2 - float)
char[] - 1 byte * N || uint64 - 8 bytes || float - 8 bytes - Label value
### Message
uint64 - 8 bytes - Message length
char[] - 1 byte * M - Message


# The data page structure optimized for:
- Insert of time series data (log, metrics, etc.)
- Fast search of time series data by timestamp
- Fast search of time series data by timestamp range
- Fast search of time series data by labels
- Slow Full Text Search (FTS) of time series data by message

# Supported operations
- Insert
- Bulk insert
- Read
- Bulk read
- Compact* TBD