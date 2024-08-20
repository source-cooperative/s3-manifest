use arrow::array::{ArrayBuilder, StringBuilder, TimestampMillisecondBuilder, UInt64Builder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use rusoto_core::{HttpClient, Region};
use rusoto_s3::{ListObjectsV2Request, Object, S3Client, S3};
use std::error::Error;
use std::fs::File;
use std::sync::Arc;
use std::time::Instant;
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tokio_retry::Retry;
use url::Url;

// Define the command-line arguments structure
#[derive(Parser, Debug)]
#[clap(author, version, about = "Generates a Parquet manifest file for an S3 bucket", long_about = None)]
struct Args {
    /// S3 URI containing both bucket and prefix (e.g., s3://bucket-name/prefix)
    s3_uri: String,

    /// Output file name for the Parquet manifest
    #[clap(short, long, default_value = "manifest.parquet")]
    output: String,

    /// Custom S3 endpoint URL (optional, use for S3-compatible services)
    #[clap(long = "endpoint-url")]
    endpoint_url: Option<String>,

    /// Delimiter to use for extracting file name (default: "/")
    #[clap(short, long, default_value = "/")]
    delimiter: String,
}

// Main function
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Parse command-line arguments
    let args = Args::parse();

    // Parse the S3 URI
    let (bucket, prefix) = parse_s3_uri(&args.s3_uri)?;

    // Call the generate_manifest function with parsed arguments
    generate_manifest(
        &bucket,
        &args.output,
        args.endpoint_url,
        prefix,
        &args.delimiter,
    )
    .await?;
    Ok(())
}

// Function to parse S3 URI
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

// Function to generate the manifest
async fn generate_manifest(
    bucket_name: &str,
    output_file: &str,
    endpoint: Option<String>,
    prefix: Option<String>,
    delimiter: &str,
) -> Result<(), Box<dyn Error>> {
    // Create S3 client
    let s3_client = create_s3_client(endpoint)?;

    // Define the schema for the Parquet file
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

    // Create the output file and Parquet writer
    let file = File::create(output_file)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

    // Initialize variables for S3 listing and data processing
    let mut continuation_token: Option<String> = None;
    let mut bucket_builder = StringBuilder::new();
    let mut key_builder = StringBuilder::new();
    let mut file_name_builder = StringBuilder::new();
    let mut size_builder = UInt64Builder::new();
    let mut last_modified_builder = TimestampMillisecondBuilder::new();

    // Set up retry strategy for S3 API calls
    let retry_strategy = ExponentialBackoff::from_millis(100).map(jitter).take(3);

    // Set up progress bar
    let pb = ProgressBar::new_spinner();
    pb.set_style(ProgressStyle::default_spinner()
        .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} ({per_sec}, {eta})")
        .progress_chars("#>-"));
    pb.set_length(0); // Indeterminate progress bar

    let start_time = Instant::now();
    let mut total_objects = 0;

    // Main loop to process S3 objects
    loop {
        let request = ListObjectsV2Request {
            bucket: bucket_name.to_string(),
            prefix: prefix.clone(), // We keep this, but it might not filter as expected
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
                // Check if the object key starts with the desired prefix
                if let Some(ref prefix) = prefix {
                    if !object
                        .key
                        .as_ref()
                        .unwrap_or(&String::new())
                        .starts_with(prefix)
                    {
                        continue; // Skip this object if it doesn't match the prefix
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

        // Update the continuation token for the next iteration
        continuation_token = result.next_continuation_token;

        // Check if there are more objects to list
        if !result.is_truncated.unwrap_or(false) {
            break;
        }

        let elapsed = start_time.elapsed();
        let objects_per_second = total_objects as f64 / elapsed.as_secs_f64();
        pb.set_message(format!("{:.2} objects/sec", objects_per_second));
    }

    // Write any remaining data
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

    // Close the Parquet writer
    writer.close()?;

    // Display final statistics
    let elapsed = start_time.elapsed();
    let objects_per_second = total_objects as f64 / elapsed.as_secs_f64();
    pb.finish_with_message(format!(
        "Done. Processed {} objects in {:.2?} ({:.2} objects/sec)",
        total_objects, elapsed, objects_per_second
    ));

    Ok(())
}

// Function to create an S3 client
fn create_s3_client(endpoint: Option<String>) -> Result<S3Client, Box<dyn Error>> {
    match endpoint {
        Some(endpoint_url) => {
            let region = Region::Custom {
                name: "custom".to_string(),
                endpoint: endpoint_url,
            };
            Ok(S3Client::new_with(
                HttpClient::new()?,
                rusoto_core::credential::StaticProvider::new_minimal(
                    "dummy".to_string(),
                    "dummy".to_string(),
                ),
                region,
            ))
        }
        None => Ok(S3Client::new(Region::default())),
    }
}

// Function to add an S3 object's data to the Arrow builders
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
    // Append the bucket name
    bucket_builder.append_value(bucket_name);

    // Extract and append the object key
    let key = object.key.as_deref().unwrap_or("");

    key_builder.append_value(key);

    // Extract and append the file name
    let file_name = key.rsplit(delimiter).next().unwrap_or(key);
    file_name_builder.append_value(file_name);

    // Append the object size
    size_builder.append_value(object.size.unwrap_or(0) as u64);

    // Parse and append the last modified timestamp
    let last_modified = object
        .last_modified
        .as_ref()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc).timestamp_millis())
        .unwrap_or(0);
    last_modified_builder.append_value(last_modified);

    Ok(())
}

// Function to write a batch of data to the Parquet file
fn write_batch(
    writer: &mut ArrowWriter<File>,
    schema: &Arc<Schema>,
    bucket_builder: &mut StringBuilder,
    key_builder: &mut StringBuilder,
    file_name_builder: &mut StringBuilder,
    size_builder: &mut UInt64Builder,
    last_modified_builder: &mut TimestampMillisecondBuilder,
) -> Result<(), Box<dyn Error>> {
    // Create a RecordBatch from the builders
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

    // Write the batch to the Parquet file
    writer.write(&batch)?;

    // Reset builders by finishing them (which clears their internal state)
    bucket_builder.finish();
    key_builder.finish();
    file_name_builder.finish();
    size_builder.finish();
    last_modified_builder.finish();

    Ok(())
}
