#!/bin/bash

# Specify the HDFS destination path
hdfs_destination_path="/path/to/hdfs/destination"

# Get the current working directory
source_directory=$(pwd)

# Get the hostname
hostname=$(hostname)

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
find "$source_directory" -maxdepth 1 -type f -name "*.log" | while read -r log_file; do
    log_filename=$(basename "$log_file")
    datetime=$(date +"%Y%m%d_%H%M%S")
    hdfs_target_path="$hdfs_destination_path/${log_filename%.*}_${hostname}_$datetime.log"

    # Upload the log file to HDFS
    hadoop fs -put "$log_file" "$hdfs_target_path"
    if [ $? -eq 0 ]; then
        echo "Uploaded $log_filename to HDFS as $hdfs_target_path."
        
        # Remove the log file from the source directory
        rm "$log_file"
        if [ $? -eq 0 ]; then
            echo "Removed $log_filename from source directory."
        else
            echo "Failed to remove $log_filename from source directory."
        fi
    else
        echo "Failed to upload $log_filename to HDFS."
    fi
done
