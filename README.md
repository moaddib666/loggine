# Loggine: The Engine of Precision and Simplicity

![Loggine Logo](./docs/assets/logo.png)

Loggine is not just another log storage engine—it's a magical tool born from the harmony of two worlds: the structured
rigor of logging and the elegance of simplicity.

Inspired by the **dry gin**, its name reflects the **DRY (Don't Repeat Yourself)** software pattern, embodying the
principle of efficiency and clarity. Just like a gin cocktail blends perfectly balanced ingredients, Loggine merges
performance, reliability, and precision, ensuring that every log is stored with purpose and efficiency.

At its core, Loggine is the **"genie"** that works behind the scenes, making log management seem almost magical. With a
misty touch of innovation, it transforms the complexity of log storage into something smooth, sophisticated, and
effortless.

Loggine thrives in environments demanding **high write throughput**, where it efficiently handles millions of log
entries per second without breaking a sweat.

- Through **intelligent compression algorithms**, it ensures that your logs take up less space without sacrificing
  detail.
- **Sharding and distribution** come naturally, scaling seamlessly across clusters to handle massive datasets with ease.

Loggine is your reliable companion—fast, scalable, and ready to deliver, one log at a time.

---

## Roadmap

### FIXME:

- [ ] Currently, the client finds only through the first match data-page, despite the fact that the range is larger and
  includes more data-pages.

### TODO:

- [ ] Introduce **Context** and gracefully shutdown the application, dumping all data to disk.
- [ ] Migrate to **Logrus**.
- [ ] Implement **.chunk merge**.
  - Merge all chunks of a day into a single file periodically.
- [ ] Add **chunk size limit**.
  - Once a chunk reaches a certain size, create a new chunk and apply the merge process.

---

## Essential

### Data Flow

We need to align the data flow so that all logs from the same time frame across different sources are written together.

#### Example:

*Given*:

- 3 sources
- **WAL** flush time - 1 minute
- **WAL** max records - 1,000
- **WAL** max size - 100MB

*When*:

- Source 1 writes logs for 00:00:00 - 00:00:59: count 325, size 10MB
- Source 2 writes logs for 00:00:00 - 00:00:59: count 325, size 10MB
- Source 3 writes logs for 00:00:00 - 00:00:59: count 325, size 10MB

*Then*:

- WAL log records:
  - 00:00:00 - 00:00:59: count 975, size 30MB
- Logs are arranged and written into 1 **DatePage** in a single data file.
- The index is updated with the new **DatePage**.

---

## Report Example

```
=========== Quick File Structure Report ===========
End of file reached.

=========== File Header ===========
Version             : 1
Id                  : 2371535632
Record Count        : 3,586,000
Data Pages Count    : 19
Checksum            : 2375124963
Date                : 2024-10-26T00:00:00Z
============================================
First Data Page Number: 1243
Last Data Page Number: 1270
First Data Page Time : 2024-10-26T20:43:00Z
Last Data Page Time  : 2024-10-26T21:10:00Z
Total Minutes        : 28

=========== Data Pages ===========
Page Number  | Page Size   | Record Count
-----------------------------------------
1243         | 6,205,016   | 20,000
1244         | 3,102,508   | 10,000
1249         | 294         | 1
1250         | 9,307,230   | 29,999
1251         | 57,805,619  | 186,270
1252         | 199,840,934 | 644,123
1253         | 55,717,412  | 179,607
1257         | 7,756,050   | 25,000
1258         | 17,683,794  | 57,000
1259         | 17,683,794  | 57,000
1260         | 17,683,794  | 57,000
1261         | 17,994,036  | 58,000
1262         | 9,927,744   | 32,000
1265         | 102,382,764 | 330,000
1266         | 139,612,860 | 450,000
1267         | 136,510,352 | 440,000
1268         | 139,441,540 | 449,447
1269         | 139,784,180 | 450,553
1270         | 34,127,588  | 110,000
============================================
```

Analysis:

- In a 1GB file, we have 3.5M records.
- The average record size is 285 bytes.

---
