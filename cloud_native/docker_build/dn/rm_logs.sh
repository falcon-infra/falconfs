#! /bin/bash
log_dir="/home/falconMeta/data/metadata/log/"
max_files=20

while true
do
    log_files=$(find "$log_dir" -type f -print0 | xargs -0 ls -1t)
    log_count=$(echo "$log_files" | wc -l)

    if [ "$log_count" -gt "$max_files" ]; then
        files_to_delete=$((log_count - max_files))

        echo "$log_files" | tail -n "$files_to_delete" | xargs -I {} rm -f {}
        echo "Deleted $files_to_delete old log files."
    else
        echo "No old log files to delete."
    fi 
    sleep 86400
done