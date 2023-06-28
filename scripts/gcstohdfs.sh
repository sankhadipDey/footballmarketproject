#!/bin/bash

# Set the necessary configurations
CLUSTER_NAME="<cluster-name>"
REGION="<region>"
BUCKET_NAME="<your-bucket>"
SCRIPT_PATH="gs://$BUCKET_NAME/path/to/gcstohdfs.py"
JARS_PATH="gs://$BUCKET_NAME/path/to/gcs-connector.jar"

# Check if the required arguments are provided
if [ "$#" -ne 2 ]; then
    echo "Usage: ./gcstohdfs.sh <csv_file_name> <hdfs_output_path>"
    exit 1
fi

# Extract the arguments
CSV_FILE_NAME=$1
HDFS_OUTPUT_PATH=$2

# Set the GCS and HDFS paths
GCS_PATH="gs://sankhadipdey_transferproject_KS/path/to/csv/$CSV_FILE_NAME"
HDFS_PATH="hdfs:///input/${CSV_FILE_NAME}.parquet"

# Build the spark-submit command
SPARK_SUBMIT_CMD="gcloud dataproc jobs submit pyspark"
SPARK_SUBMIT_CMD+=" --cluster $CLUSTER_NAME"
SPARK_SUBMIT_CMD+=" --region $REGION"
SPARK_SUBMIT_CMD+=" --jars $JARS_PATH"
SPARK_SUBMIT_CMD+=" -- $SCRIPT_PATH $CSV_FILE_NAME $HDFS_OUTPUT_PATH"

# Execute the spark-submit command
eval "$SPARK_SUBMIT_CMD"
