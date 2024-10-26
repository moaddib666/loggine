# Log

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

