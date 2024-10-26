# DB Index Structure

## Index By Date

In database date is a special field that saved in uint64 format of unix timestamp.
The index by date is a B+Tree index that contains the date field as a key and the page number as a value.
The index is used to search the data pages by date -> hour -> minute
The index is distributed across shards and each shard contains a part of the index.

### Use Cases

Given:

- 1 controller node
- 3 shards = 3 Data nodes
- 10 day of apache logs
- 10 files per day * 3 shards = 30 files

When:

- Search for logs between 2021-01-01 00:00:00 and 2021-01-01 00:00:59

Then:

- The request will be sent to the controller node
- The controller node will search for the index of the date 2021-01-01 present in the 3 shards
- Each shard will search for the index of the date 2021-01-01
- Each shard will read the data pages of the date 2021-01-01
- Each shard will search for the data pages of the hour 00
- Each shard will search for the data pages of the minute 00
- Each shard will read the data pages of the minute 00
- Controller node will merge the results order by DESC and return the response

Finally:

- The controller node will return the logs between 2021-01-01 00:00:00 and 2021-01-01 00:00:59


