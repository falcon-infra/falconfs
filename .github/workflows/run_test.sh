#!/bin/bash
set -euo pipefail

CURDIR=$(pwd)
source $CURDIR/deploy/falcon_env.sh
source $CURDIR/deploy/client/falcon_client_config.sh
$FALCONFS_HOME/build.sh test
$FALCONFS_HOME/deploy/falcon_start.sh
$FALCONFS_HOME/.github/workflows/smoke_test.sh $MNT_PATH
$FALCONFS_HOME/deploy/falcon_stop.sh
$FALCONFS_HOME/build.sh clean
$FALCONFS_HOME/build.sh clean dist
