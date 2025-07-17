#!/bin/bash

set -euo pipefail

usage() {
    echo "Usage: $0 <link_record_file>"
    exit 1
}

if [ $# -ne 1 ]; then
    usage
fi

LINK_RECORD_FILE="$1"

if [ ! -f "$LINK_RECORD_FILE" ]; then
    echo "'$LINK_RECORD_FILE' doesn't exist"
    exit 1
fi

while IFS= read -r link_path; do
    if [ -L "$link_path" ]; then
        unlink "$link_path"
    else
        echo "Error: $link_path is not a soft link"
    fi
done < "$LINK_RECORD_FILE"

echo "Done"
