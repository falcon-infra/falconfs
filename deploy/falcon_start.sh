#!/usr/bin/env bash
DIR=$(dirname $(readlink -f "${BASH_SOURCE[0]}"))

source $DIR/falcon_env.sh

COMM_PLUGIN="brpc"
for arg in "$@"; do
    case "$arg" in
        --comm-plugin=*)
            COMM_PLUGIN="${arg#*=}"
            ;;
    esac
done

if ! $DIR/meta/falcon_meta_start.sh "$@"; then
    echo "Error: falcon_meta_start failed, skip falcon_client_start" >&2
    exit 1
fi

if [[ "$COMM_PLUGIN" == "hcom" ]]; then
    echo "Skip falcon_client_start for hcom mode"
    exit 0
fi

sleep 3
$DIR/client/falcon_client_start.sh
