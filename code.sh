#!/bin/bash

# Specify the source directory and HDFS destination path
source_directory="/path/to/source/directory"
hdfs_destination_path="/path/to/hdfs/destination"

# Check if the source directory exists
if [ ! -d "$source_directory" ]; then
    echo "Source directory does not exist: $source_directory"
    exit 1
fi

# Check if HDFS destination path exists, create if not
hadoop fs -test -e "$hdfs_destination_path"
if [ $? -ne 0 ]; then
    echo "HDFS destination path does not exist. Creating path: $hdfs_destination_path"
    hadoop fs -mkdir -p "$hdfs_destination_path"
    if [ $? -ne 0 ]; then
        echo "Failed to create HDFS destination path."
        exit 1
    fi
fi

# Find and transfer .log files to HDFS
find "$source_directory" -type f -name "*.log" | while read -r log_file; do
    log_filename=$(basename "$log_file")
    datetime=$(date +"%Y%m%d_%H%M%S")
    hdfs_target_path="$hdfs_destination_path/${log_filename%.*}_$datetime.log"

    # Upload the log file to HDFS
    hadoop fs -put "$log_file" "$hdfs_target_path"
    if [ $? -eq 0 ]; then
        echo "Uploaded $log_filename to HDFS as $hdfs_target_path."
    else
        echo "Failed to upload $log_filename to HDFS."
    fi
done
