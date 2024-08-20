# S3 Manifest Generator

S3 Manifest Generator is a Rust-based command-line tool that creates a Parquet manifest file for objects in an S3 bucket or a specific prefix within a bucket. This tool is useful for quickly generating an inventory of S3 objects, including their metadata, in a compact and efficiently queryable format.

## Features

- Generate a Parquet manifest file for S3 objects
- Support for custom S3-compatible endpoints for both source and destination
- Configurable delimiter for file name extraction
- Progress bar with real-time statistics
- Retry mechanism for S3 API calls
- Efficient batch processing of S3 objects
- Option to output to local file or directly to S3
- Support for separate credentials for source and destination buckets

## Installation

To install the S3 Manifest Generator, you need to have Rust and Cargo installed on your system. Then, you can build the project from source:

```bash
git clone https://github.com/source-cooperative/s3-manifest.git
cd s3-manifest
cargo build --release
```

The compiled binary will be available in the `target/release` directory.

## Usage

```bash
s3-manifest [OPTIONS] <S3_URI>
```

### Arguments

- `<S3_URI>`: S3 URI containing both bucket and prefix (e.g., s3://bucket-name/prefix)

### Options

- `-o, --output <OUTPUT>`: Output file name for the Parquet manifest (local path or S3 URI)
- `--source-endpoint <SOURCE_ENDPOINT>`: Custom S3 endpoint URL for source bucket (optional, use for S3-compatible services)
- `--dest-endpoint <DEST_ENDPOINT>`: Custom S3 endpoint URL for destination bucket (optional, use for S3-compatible services)
- `-d, --delimiter <DELIMITER>`: Delimiter to use for extracting file name [default: "/"]
- `--source-access-key <SOURCE_ACCESS_KEY>`: AWS Access Key ID for the source bucket
- `--source-secret-key <SOURCE_SECRET_KEY>`: AWS Secret Access Key for the source bucket
- `--dest-access-key <DEST_ACCESS_KEY>`: AWS Access Key ID for the destination bucket
- `--dest-secret-key <DEST_SECRET_KEY>`: AWS Secret Access Key for the destination bucket
- `-h, --help`: Print help information
- `-V, --version`: Print version information

### Example

```bash
s3-manifest s3://my-bucket/my-prefix -o s3://output-bucket/my-manifest.parquet --delimiter "/" --source-endpoint https://custom-s3.example.com
```

This command will generate a Parquet manifest file named `my-manifest.parquet` in the `output-bucket` S3 bucket for all objects in the `my-prefix` of the `my-bucket` S3 bucket, using "/" as the delimiter for file name extraction and a custom S3 endpoint for the source bucket.

## Output

The generated Parquet file contains the following columns:

- Bucket: The name of the S3 bucket
- Key: The full key of the S3 object
- FileName: The extracted file name based on the specified delimiter
- Size: The size of the object in bytes
- LastModified: The last modified timestamp of the object

## Dependencies

This project relies on several Rust crates, including:

- arrow
- chrono
- clap
- futures
- indicatif
- parquet
- rusoto_core
- rusoto_s3
- tokio
- url
- tempfile
- tokio-retry
- bytes

For a complete list of dependencies and their versions, please refer to the `Cargo.toml` file.

## License

[MIT License](LICENSE)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
