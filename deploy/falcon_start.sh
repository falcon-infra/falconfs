#!/usr/bin/env bash
DIR=$(dirname $(readlink -f "${BASH_SOURCE[0]}"))

source $DIR/falcon_env.sh
if ! $DIR/meta/falcon_meta_start.sh "$@"; then
    echo "Error: falcon_meta_start failed, skip falcon_client_start" >&2
    exit 1
fi
sleep 3
$DIR/client/falcon_client_start.sh
