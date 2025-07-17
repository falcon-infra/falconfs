#!/bin/bash
set -euo pipefail

usage() {
    echo "Usage: $0 <source_directory> <destination_directory>"
    exit 1
}

if [ $# -ne 2 ]; then
    usage
fi

SRC="$1"
DST="$2"

LINK_RECORD_FILE="$SRC"/link_record_$(date +%Y%m%d_%H%M%S).log

if [ ! -d "$SRC" ]; then
    echo "Source directory '$SRC' is not a directory." >&2
    exit 2
fi

if [ ! -e "$DST" ]; then
    mkdir -p "$DST"
fi
if [ ! -d "$DST" ]; then
    echo "Destination directory '$DST' does not exist or is not a directory." >&2
    exit 3
fi

is_python_lib_dir() {
    local path="$1"

    local base=$(basename "$path")
    local parent_base=$(basename "$(dirname "$path")")
    if [[ "$parent_base" == "lib" && "$base" =~ ^python[0-9]+\.[0-9]+$ ]]; then
        return 0
    else
        return 1
    fi
}

is_cmake_special_dir() {
    local path="$1"
    local base=$(basename "$path")
    local parent_base=$(basename "$(dirname "$path")")

    if [[ "$base" =~ ^cmake-[0-9]+\.[0-9]+$ && ("$parent_base" == "share" || "$parent_base" == "doc") ]]; then
        return 0
    else
        return 1
    fi
}

link_recursive() {
    local src_path="$1"
    local dst_path="$2"

    # for special directories
    if is_python_lib_dir "$src_path" || is_cmake_special_dir "$src_path"; then
        if [ ! -e "$dst_path" ]; then
            local rel_link
            rel_link=$(realpath --relative-to="$(dirname "$dst_path")" "$src_path")
            ln -s "$rel_link" "$dst_path"
            echo "Linked site-packages directory: $src_path -> $dst_path"
            echo "$dst_path" >> "$LINK_RECORD_FILE"
        else
            echo "Error: $dst_path already exists" >&2
        fi
        return
    fi

    if [ -d "$src_path" ] && [ ! -L "$src_path" ]; then
        if [ ! -d "$dst_path" ]; then
            mkdir -p "$dst_path"
            echo "Created directory: $dst_path"
        fi
        for item in "$src_path"/*; do
            if [ ! -e "$item" ]; then
                echo "Error: $item link to a non-exist path"
                continue
            fi
            local base_item
            base_item=$(basename "$item")
            link_recursive "$item" "$dst_path/$base_item"
        done
    else
        if [ ! -e "$dst_path" ]; then
            local rel_link
            rel_link=$(realpath --relative-to="$(dirname "$dst_path")" "$src_path")
            ln -s "$rel_link" "$dst_path"
            echo "Linked file: $src_path -> $dst_path"

            echo "$dst_path" >> "$LINK_RECORD_FILE"
        else
            echo "Error: $dst_path already exists" >&2
        fi
    fi
}

for entry in "$SRC"/*; do
    [ -e "$entry" ] || continue
    base=$(basename "$entry")
    link_recursive "$entry" "$DST/$base"
done
