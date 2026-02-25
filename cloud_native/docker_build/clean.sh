#! /bin/bash
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

pushd $DIR
rm -rf ./cn/falconfs
rm -rf ./dn/falconfs
rm -rf ./store/falconfs
popd