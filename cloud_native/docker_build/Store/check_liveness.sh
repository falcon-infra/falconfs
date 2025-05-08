#!/bin/bash

isProcessAlive=`ps -ux | grep falcon_client | grep -v "grep" | wc -l`
if [ "${isProcessAlive}" = "0" ]; then
    exit 1;
else
    exit 0;
fi