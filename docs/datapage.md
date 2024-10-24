# Data page format
The Data page is a file that contains the data of the database. 
It is a binary file that contains the data of the database.
The data page is divided into two parts, the header and the data. 
The header contains the metadata of the page, and the data contains the records of the page.

```
    |------------File1-------------|
    | HEADER                       |      
    |   |-----PAGE-----------|     |
    |   | HEADER             |     |
    |   | RECORD  1          |     |
    |   | ...                |     |
    |   | RECORD  2          |     |
    |   |--------------------|     |
    |   | PAGE               |     |
    |   | HEADER             |     |
    |   | RECORD  1          |     |
    |   | ...                |     |
    |   | RECORD  2          |     |
    |   |--------------------|     |
    |   | ...                |     |
    |   |--------------------|     |
    |______________________________|
    
    |------------File2-------------|
    | HEADER                       |      
    |   |-----PAGE-----------|     |
    |   | HEADER             |     |
    |   | RECORD  1          |     |
    |   | ...                |     |
    |   | RECORD  2          |     |
    |   |--------------------|     |
    |   | PAGE               |     |
    |   | HEADER             |     |
    |   | RECORD  1          |     |
    |   | ...                |     |
    |   | RECORD  2          |     |
    |   |--------------------|     |
    |   | ...                |     |
    |   |--------------------|     |
    |______________________________|
 ```

## File Header
Version (uint64) - 8 bytes
Page number (uint64) - 8 bytes
Record count (uint64) - 8 bytes
Year (uint64) - 8 bytes
Month (uint64) - 8 bytes
Day (uint64) - 8 bytes
Reserved (256 bytes) - 256 bytes (Reserved for future use, padding, etc.)
Checksum (uint64) - 8 bytes
Total Header Size = 8 + 8 + 8 + 8 + 8 + 8 + 256 + 8 = 312 bytes

## Page

### Page Header - Minute representation
uint32 - 4 byte - Minute number in 24 hours
uint64 - 8 bytes - Page Size
uint64 - 8 bytes - Record count

Total Minute Data Size (with uint8 for minute number) = 1 + 8 + 8 + (Record Size * Record Count)


### Page Record - Minute log data
#### Record Meta - 64 bytes
uint64 - 8 bytes - Timestamp
uint64 - 8 bytes - Record size
uint64 - 8 bytes - Schema Version
uint64 - 8 bytes - Labels size
uint64 - 8 bytes - Labels count
uint64 - 8 bytes - Message size
#### Record Labels - N bytes
uint64 - 8 bytes - label type (0 - string, 1 - int, 2 - float)
uint64 - 8 bytes - Label value size
char[] - 1 byte * N || uint64 - 8 bytes || float - 8 bytes - Label value
### Record Message - M bytes
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