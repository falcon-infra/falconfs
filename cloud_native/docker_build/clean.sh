#! /bin/bash
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

pushd $DIR
rm -rf ./cn/falcon_cm
rm -rf ./cn/metadb
rm -rf ./dn/falcon_cm
rm -rf ./dn/metadb
rm -rf ./store/falconfs
popd