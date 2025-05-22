# S3 Stream Archiver

This tool enables efficient, on-the-fly compression and archiving of content from an Amazon S3 bucket without using local disk space. It streams objects matching a specified prefix from a source S3 bucket, compresses them into a ZIP file in real time, and uploads the resulting archive directly to a destination S3 bucket.

## Features

- **No Local Disk Usage:** Streams data directly between S3 buckets without writing files to local storage.
- **Prefix Support:** Selects and archives only the objects matching a given prefix in the source bucket.
- **On-the-fly Compression:** Compresses files into a ZIP archive as they are streamed.
- **Flexible Destination:** Stores the resulting ZIP file at any specified location in a destination S3 bucket.

## Usage

1. **Configure Source and Destination:**
   - Specify the source S3 bucket and prefix to select files.
   - Specify the destination S3 bucket and key for the ZIP archive.

2. **Run the Tool:**
   - The tool will stream objects from the source, compress them, and upload the ZIP file to the destination without using local disk space.

## Example

- Archive all files under `logs/2024/` in `my-source-bucket` and store the ZIP as `archives/logs-2024.zip` in `my-destination-bucket`.
```bash
./s3-archiver -source-bucket my-source-bucket -source-prefix logs/2024/ -dest-bucket my-destionation-bucket -dest-key logs-2024.zip
```

## License

MIT License

---