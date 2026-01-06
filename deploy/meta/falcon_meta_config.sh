#!/bin/bash
localIp='127.0.0.1'
cnIp='127.0.0.1'
workerIpList=('127.0.0.1')
workerNumList=(2)

uniquePortPrefix=555
cnPortPrefix=${uniquePortPrefix}0
cnPoolerPortPrefix=${uniquePortPrefix}1
workerPortPrefix=${uniquePortPrefix}2
workerPollerPortPrefix=${uniquePortPrefix}3
cnMonitorPortPrefix=${uniquePortPrefix}8
workerMonitorPortPrefix=${uniquePortPrefix}9

workspace=$HOME
cnPathPrefix=$workspace/metadata/coordinator
workerPathPrefix=$workspace/metadata/worker

clean_metadata_dirs() {
    echo "Cleaning metadata directories..."
    rm -rf "$workspace/metadata"
    echo "Metadata directories cleaned"
}
