#! /bin/bash
if [ ! -d "/home/falconMeta/data/metadata" ]; then
    chown falconMeta:falconMeta /home/falconMeta/data
    chmod 777 /home/falconMeta/data
    mkdir /home/falconMeta/data/metadata
    chown falconMeta:falconMeta /home/falconMeta/data/metadata
    chmod 777 /home/falconMeta/data/metadata
fi

su falconMeta -c "bash /home/falconMeta/start.sh"
