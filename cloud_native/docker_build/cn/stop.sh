#! /bin/bash
killall python3
psql -d postgres -c "CHECKPOINT;"
sleep 1
pg_ctl stop -m immediate -D /home/falconMeta/data/metadata