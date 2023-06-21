from pyspark.sql import SparkSession
import sys

class GcsToHdfs:
    def __init__(self, gcs_path, hdfs_path):
        self.gcs_path = gcs_path
        self.hdfs_path = hdfs_path
        self.spark = SparkSession.builder \
            .appName("CSV to Parquet Conversion") \
            .getOrCreate()

    def convert(self):
        try:
            df = self.spark.read.format("csv") \
                .option("header", "true") \
                .load(self.gcs_path)

            df.write.mode("overwrite") \
                .parquet(self.hdfs_path)
				.option("compression","snappy")
            print("Conversion completed successfully.")
        except Exception as e:
            print("Error during conversion:", str(e))
        finally:
            self.spark.stop()

if __name__ == "__main__":
    # Get the CSV file name and HDFS output path from command-line arguments
    if len(sys.argv) != 3:
        print("Usage: python script.py <csv_file_name> <hdfs_output_path>")
        sys.exit(1)

    csv_file_name = sys.argv[1]
    hdfs_output_path = sys.argv[2]

    # Set GCS and HDFS paths
    gcs_path = f"gs://sankhadipdey_transferproject_KS/path/to/csv/{csv_file_name}"
    hdfs_path = f"hdfs:///path/to/parquet/output/{csv_file_name}.parquet"

    # Create an instance of GcsToHdfs
    converter = GcsToHdfs(gcs_path, hdfs_path)

    # Convert CSV to Parquet
    converter.convert()
