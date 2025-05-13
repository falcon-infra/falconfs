#!/bin/bash

CURDIR=$(pwd)
source $CURDIR/deploy/falcon_env.sh

set -euo pipefail

$FALCONFS_HOME/build.sh test
$FALCONFS_HOME/deploy/falcon_start.sh
$FALCONFS_HOME/.github/workflows/smoke_test.sh $MNT_PATH
$FALCONFS_HOME/deploy/falcon_stop.sh
$FALCONFS_HOME/build.sh clean
$FALCONFS_HOME/build.sh clean dist
