use arrow::array::{ArrayBuilder, StringBuilder, TimestampMillisecondBuilder, UInt64Builder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use rusoto_core::credential::{ChainProvider, StaticProvider};
use rusoto_core::ByteStream;
use rusoto_core::{HttpClient, Region};
use rusoto_s3::{ListObjectsV2Request, Object, PutObjectRequest, S3Client, S3};
use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::sync::Arc;
use std::time::Instant;
use tempfile::NamedTempFile;
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tokio_retry::Retry;
use url::Url;

#[derive(Parser, Debug)]
#[clap(author, version, about = "Generates a Parquet manifest file for an S3 bucket", long_about = None)]
struct Args {
    /// S3 URI containing both bucket and prefix (e.g., s3://bucket-name/prefix)
    s3_uri: String,

    /// Output file name for the Parquet manifest (local path or S3 URI)
    #[clap(short, long)]
    output: String,

    /// Custom S3 endpoint URL for source bucket (optional, use for S3-compatible services)
    #[clap(long = "source-endpoint")]
    source_endpoint: Option<String>,

    /// Custom S3 endpoint URL for destination bucket (optional, use for S3-compatible services)
    #[clap(long = "dest-endpoint")]
    dest_endpoint: Option<String>,

    /// Delimiter to use for extracting file name (default: "/")
    #[clap(short, long, default_value = "/")]
    delimiter: String,

    /// AWS Access Key ID for the source bucket
    #[clap(long = "source-access-key")]
    source_access_key: Option<String>,

    /// AWS Secret Access Key for the source bucket
    #[clap(long = "source-secret-key")]
    source_secret_key: Option<String>,

    /// AWS Access Key ID for the destination bucket
    #[clap(long = "dest-access-key")]
    dest_access_key: Option<String>,

    /// AWS Secret Access Key for the destination bucket
    #[clap(long = "dest-secret-key")]
    dest_secret_key: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    let (bucket, prefix) = parse_s3_uri(&args.s3_uri)?;
    let (output_bucket, output_key) = parse_output_location(&args.output)?;

    generate_manifest(
        &bucket,
        &args.output,
        args.source_endpoint,
        args.dest_endpoint,
        prefix,
        &args.delimiter,
        output_bucket,
        output_key,
        args.source_access_key,
        args.source_secret_key,
        args.dest_access_key,
        args.dest_secret_key,
    )
    .await?;
    Ok(())
}

fn parse_s3_uri(uri: &str) -> Result<(String, Option<String>), Box<dyn Error>> {
    let parsed_url = Url::parse(uri)?;

    if parsed_url.scheme() != "s3" {
        return Err("Invalid S3 URI scheme. Must start with 's3://'.".into());
    }

    let bucket = parsed_url
        .host_str()
        .ok_or("Missing bucket name")?
        .to_string();
    let prefix = if parsed_url.path().len() > 1 {
        Some(parsed_url.path()[1..].to_string())
    } else {
        None
    };

    Ok((bucket, prefix))
}

fn parse_output_location(output: &str) -> Result<(Option<String>, String), Box<dyn Error>> {
    if output.starts_with("s3://") {
        let parsed_url = Url::parse(output)?;
        let bucket = parsed_url
            .host_str()
            .ok_or("Missing bucket name")?
            .to_string();
        let key = parsed_url.path().trim_start_matches('/').to_string();
        Ok((Some(bucket), key))
    } else {
        Ok((None, output.to_string()))
    }
}

async fn generate_manifest(
    bucket_name: &str,
    output_file: &str,
    source_endpoint: Option<String>,
    dest_endpoint: Option<String>,
    prefix: Option<String>,
    delimiter: &str,
    output_bucket: Option<String>,
    output_key: String,
    source_access_key: Option<String>,
    source_secret_key: Option<String>,
    dest_access_key: Option<String>,
    dest_secret_key: Option<String>,
) -> Result<(), Box<dyn Error>> {
    let s3_client = create_s3_client(source_endpoint, source_access_key, source_secret_key)?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("Bucket", DataType::Utf8, false),
        Field::new("Key", DataType::Utf8, false),
        Field::new("FileName", DataType::Utf8, false),
        Field::new("Size", DataType::UInt64, false),
        Field::new(
            "LastModified",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            false,
        ),
    ]));

    let (mut writer, temp_file) = if output_bucket.is_some() {
        let temp_file = NamedTempFile::new()?;
        let props = WriterProperties::builder().build();
        let writer = ArrowWriter::try_new(
            Box::new(temp_file.reopen()?) as Box<dyn std::io::Write + Send>,
            schema.clone(),
            Some(props),
        )?;
        (writer, Some(temp_file))
    } else {
        let file = File::create(output_file)?;
        let props = WriterProperties::builder().build();
        let writer = ArrowWriter::try_new(
            Box::new(file) as Box<dyn std::io::Write + Send>,
            schema.clone(),
            Some(props),
        )?;
        (writer, None)
    };

    let mut continuation_token: Option<String> = None;
    let mut bucket_builder = StringBuilder::new();
    let mut key_builder = StringBuilder::new();
    let mut file_name_builder = StringBuilder::new();
    let mut size_builder = UInt64Builder::new();
    let mut last_modified_builder = TimestampMillisecondBuilder::new();

    let retry_strategy = ExponentialBackoff::from_millis(100).map(jitter).take(3);

    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} [{elapsed_precise}] {pos} Objects Scanned ({per_sec})")
            .progress_chars("#>-"),
    );
    pb.set_length(0);

    let start_time = Instant::now();
    let mut total_objects = 0;

    loop {
        let request = ListObjectsV2Request {
            bucket: bucket_name.to_string(),
            prefix: prefix.clone(),
            continuation_token: continuation_token.clone(),
            max_keys: Some(1000),
            ..Default::default()
        };

        let result = Retry::spawn(retry_strategy.clone(), || {
            let s3_client = s3_client.clone();
            let request = request.clone();
            async move {
                s3_client.list_objects_v2(request).await.map_err(|e| {
                    println!("Error listing objects, retrying: {:?}", e);
                    e
                })
            }
        })
        .await?;

        if let Some(objects) = result.contents {
            for object in objects {
                if let Some(ref prefix) = prefix {
                    if !object
                        .key
                        .as_ref()
                        .unwrap_or(&String::new())
                        .starts_with(prefix)
                    {
                        continue;
                    }
                }

                add_object_to_builders(
                    bucket_name,
                    &object,
                    delimiter,
                    &mut bucket_builder,
                    &mut key_builder,
                    &mut file_name_builder,
                    &mut size_builder,
                    &mut last_modified_builder,
                )?;
                total_objects += 1;
                pb.set_position(total_objects);
            }
        }

        if key_builder.len() >= 1000 {
            write_batch(
                &mut writer,
                &schema,
                &mut bucket_builder,
                &mut key_builder,
                &mut file_name_builder,
                &mut size_builder,
                &mut last_modified_builder,
            )?;
        }

        continuation_token = result.next_continuation_token;

        if !result.is_truncated.unwrap_or(false) {
            break;
        }

        let elapsed = start_time.elapsed();
        let objects_per_second = total_objects as f64 / elapsed.as_secs_f64();
        pb.set_message(format!("{:.2} objects/sec", objects_per_second));
    }

    if key_builder.len() > 0 {
        write_batch(
            &mut writer,
            &schema,
            &mut bucket_builder,
            &mut key_builder,
            &mut file_name_builder,
            &mut size_builder,
            &mut last_modified_builder,
        )?;
    }

    writer.close()?;

    if let Some(temp_file) = temp_file {
        let dest_s3_client = create_s3_client(dest_endpoint, dest_access_key, dest_secret_key)?;
        upload_to_s3(
            &dest_s3_client,
            temp_file,
            &output_bucket.unwrap(),
            &output_key,
        )
        .await?;
    }

    let elapsed = start_time.elapsed();
    let objects_per_second = total_objects as f64 / elapsed.as_secs_f64();
    pb.finish_with_message(format!(
        "Done. Processed {} objects in {:.2?} ({:.2} objects/sec)",
        total_objects, elapsed, objects_per_second
    ));

    Ok(())
}

fn create_s3_client(
    endpoint: Option<String>,
    access_key: Option<String>,
    secret_key: Option<String>,
) -> Result<S3Client, Box<dyn Error>> {
    let region = match endpoint {
        Some(endpoint_url) => Region::Custom {
            name: "custom".to_string(),
            endpoint: endpoint_url,
        },
        None => Region::default(),
    };

    match (access_key, secret_key) {
        (Some(access_key), Some(secret_key)) => Ok(S3Client::new_with(
            HttpClient::new()?,
            StaticProvider::new_minimal(access_key, secret_key),
            region,
        )),
        _ => Ok(S3Client::new_with(
            HttpClient::new()?,
            ChainProvider::new(),
            region,
        )),
    }
}

async fn upload_to_s3(
    s3_client: &S3Client,
    temp_file: tempfile::NamedTempFile,
    bucket: &str,
    key: &str,
) -> Result<(), Box<dyn Error>> {
    let mut file = temp_file.reopen()?;
    let mut contents = Vec::new();
    file.read_to_end(&mut contents)?;

    let retry_strategy = ExponentialBackoff::from_millis(100).map(jitter).take(3);

    // Create a Vec<u8> from the contents
    let contents_vec = contents.to_vec();

    let _result = Retry::spawn(retry_strategy, || {
        let s3_client = s3_client.clone();
        let bucket = bucket.to_string();
        let key = key.to_string();
        let contents = contents_vec.clone();

        async move {
            let put_request = PutObjectRequest {
                bucket: bucket,
                key: key,
                body: Some(ByteStream::from(contents)),
                ..Default::default()
            };

            s3_client.put_object(put_request).await.map_err(|e| {
                println!("Error uploading object, retrying: {:?}", e);
                e
            })
        }
    })
    .await?;

    Ok(())
}

fn add_object_to_builders(
    bucket_name: &str,
    object: &Object,
    delimiter: &str,
    bucket_builder: &mut StringBuilder,
    key_builder: &mut StringBuilder,
    file_name_builder: &mut StringBuilder,
    size_builder: &mut UInt64Builder,
    last_modified_builder: &mut TimestampMillisecondBuilder,
) -> Result<(), Box<dyn Error>> {
    bucket_builder.append_value(bucket_name);

    let key = object.key.as_deref().unwrap_or("");
    key_builder.append_value(key);

    let file_name = key.rsplit(delimiter).next().unwrap_or(key);
    file_name_builder.append_value(file_name);

    size_builder.append_value(object.size.unwrap_or(0) as u64);

    let last_modified = object
        .last_modified
        .as_ref()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc).timestamp_millis())
        .unwrap_or(0);
    last_modified_builder.append_value(last_modified);

    Ok(())
}

fn write_batch(
    writer: &mut ArrowWriter<Box<dyn std::io::Write + Send>>,
    schema: &Arc<Schema>,
    bucket_builder: &mut StringBuilder,
    key_builder: &mut StringBuilder,
    file_name_builder: &mut StringBuilder,
    size_builder: &mut UInt64Builder,
    last_modified_builder: &mut TimestampMillisecondBuilder,
) -> Result<(), Box<dyn Error>> {
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(bucket_builder.finish()),
            Arc::new(key_builder.finish()),
            Arc::new(file_name_builder.finish()),
            Arc::new(size_builder.finish()),
            Arc::new(last_modified_builder.finish()),
        ],
    )?;

    writer.write(&batch)?;

    bucket_builder.finish();
    key_builder.finish();
    file_name_builder.finish();
    size_builder.finish();
    last_modified_builder.finish();

    Ok(())
}
