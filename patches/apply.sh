#! /bin/bash 
set -e
cd "$(dirname "$0")"

if git -C ../third_party/postgres/ apply --reverse --check ../../patches/postgres.patch &> /dev/null; then
    echo "postgres patch already applied. skipping."
else
    git -C ../third_party/postgres/ apply ../../patches/postgres.patch
fi

if git -C ../third_party/postgres/ apply --reverse --check ../../patches/postgresql_RelationTruncate_bugfix.patch &> /dev/null; then
    echo "postgresql_RelationTruncate_bugfix patch already applied. skipping."
else
    git -C ../third_party/postgres/ apply ../../patches/postgresql_RelationTruncate_bugfix.patch
fi
