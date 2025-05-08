#!/bin/bash
pg_isready -d postgres -U falconMeta --timeout=5 --quiet
if [ $? != 0 ]; then
    exit 1;
fi

monitorAlive=`ps aux | grep python3 | grep -v grep | wc -l`
if [ "${monitorAlive}" = "0" ]; then
    exit 1;
else
    exit 0;
fi