Examples:
select * from audit.logs partion shard1 where timistamp > "TS RFC UTC" and timistamp < "TS RFC UTC" and label.foo exist
and label.bar is not null and label.baz = "fiz" limit 1000 aggregated by minute fromat json;
scan audit.logs;

{operation} {fields} from {database}.{table} (partition {partitonLabel})? (where {field} {operator} {condition} (and
{field} {operator} {condition})?)? (limit {value})? (aggregated by {dimension})? (fromat json)?;

# operation

- select
- scan

# fields

-
    *
- []string{timestamp, labels.*, message}

# operator

- >
- <
- =
- > =
- <=
- not {operator}
- exist
- is null
- and {operator}
- ([]operator)

#dimension

- minute
- hour
- day
- week
- month
- quarter
- year

# format

text
csv
json
yaml
binary