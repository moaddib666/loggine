### Analysis of Compression Results:

1. **Gzip**:
    - **Compression Ratio**: 10.86
    - **Time Taken**: 35.64 seconds
    - **Summary**: Gzip offers a decent compression ratio but takes significantly longer to compress. It's a
      well-balanced algorithm that trades off compression time for a good compression ratio. It may be a better choice
      if space is more important than speed.

2. **LZ4**:
    - **Compression Ratio**: 5.92
    - **Time Taken**: 4.74 seconds
    - **Summary**: LZ4 is known for its speed, and as expected, it compressed much faster than Gzip (around 7.5x
      faster). However, its compression ratio is lower, so it's more suitable for scenarios where compression speed is
      critical but space savings are less important.

3. **Snappy**:
    - **Compression Ratio**: 5.16
    - **Time Taken**: 3.35 seconds
    - **Summary**: Snappy provides slightly worse compression ratios than LZ4 but is slightly faster. It is commonly
      used in databases like Cassandra, as it's designed for fast, lightweight compression. Use this if you need high
      throughput but still want some space savings.

4. **Zstandard (Zstd)**:
    - **Compression Ratio**: 19.91
    - **Time Taken**: 3.42 seconds
    - **Summary**: Zstd provides **the best compression ratio** by far, reducing the file size by almost 20x. Despite
      this impressive ratio, it also maintains a very fast compression time, similar to Snappy and LZ4. Zstd is often
      considered the best general-purpose compressor, offering both speed and excellent compression.

### When to Use Each Compressor:

- **Gzip**: Use when disk space is a concern and time is not critical. It’s great for archiving files and compressing
  web traffic (HTTP).

- **LZ4**: Best for real-time systems where speed is more critical than compression ratio. It’s ideal for log storage,
  stream processing, and in-memory databases.

- **Snappy**: Similar to LZ4, Snappy is used for fast, low-overhead compression, especially in high-throughput
  environments like databases.

- **Zstandard (Zstd)**: The best overall compressor when you want both good compression ratios and speed. It is ideal
  for scenarios where you need to compress large volumes of data and you care about both space and time (e.g., backups,
  large log files).

### 1 GB Data File Compression Results:

```
113M    ./compressed_files/2024-10-26.2371535632.chunk.gz
192M    ./compressed_files/2024-10-26.2371535632.chunk.lz4
208M    ./compressed_files/2024-10-26.2371535632.chunk.snappy
54M    ./compressed_files/2024-10-26.2371535632.chunk.zstd
1.0G    ./decompressed_files/2024-10-26.2371535632.chunk_decompressed
```