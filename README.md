# Log

## FIXME:

- Right now client is find only trough only first match data-page despite the facto that range is bigger and include
  more data-pages
-

## TODO:

- Introduce Context and gracefully shutdown application with dump of all data to disk.
- Todo Migrate to Logrus
- Todo implement .chunk merge
  - Once a timeframe we need to merge all chunks of a day into a single file
- Todo chunk size limit
  - Once a chunk reaches a certain size we need to create a new chunk apliying to merge as well

## Essential:

### Data Flow

Meed align data flow so that all TimeFrame logs from all sources been written in same timeframe
Example:
*Given*:

- 3 sources
- WAL flush time - 1m
- WAL max records - 1000
- WAL max size - 100MB
  *When*:
- 1 source write log in range 00:00:00 - 00:00:59 count 325 size 10MB
- 2 source write log in range 00:00:00 - 00:00:59 count 325 size 10MB
- 3 source write log in range 00:00:00 - 00:00:59 count 325 size 10MB
  *Then*:
- Log records in WAL:
- 00:00:00 - 00:00:59 count 975 size 30MB
- Logs arranged and written in 1 DatePage in single data file
- Index updated with new DatePage

### Report Example

=========== Quick File Structure Report ===========
End of file reached.

=========== File Header ===========
Version             : 1
Id                  : 2371535632
Record Count        : 3586000
Data Pages Count    : 19
Checksum            : 2375124963
Date                : 2024-10-26T00:00:00Z
============================================
First Data Page Number: 1243
Last Data Page Number: 1270
First Data Page Time: 2024-10-26T20:43:00Z
Last Data Page Time : 2024-10-26T21:10:00Z
Total Minutes       : 28

=========== Data Pages ===========
Page Number Page Size Record Count  
1243 6205016 20000         
1244 3102508 10000         
1249 294 1             
1250 9307230 29999         
1251 57805619 186270        
1252 199840934 644123        
1253 55717412 179607        
1257 7756050 25000         
1258 17683794 57000         
1259 17683794 57000         
1260 17683794 57000         
1261 17994036 58000         
1262 9927744 32000         
1265 102382764 330000        
1266 139612860 450000        
1267 136510352 440000        
1268 139441540 449447        
1269 139784180 450553        
1270 34127588 110000        
============================================

Analisys:

- In 1 GB file we have 3.5M records
- Average record size is 285 bytes
