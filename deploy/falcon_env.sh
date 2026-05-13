#!/usr/bin/env bash

export FALCONFS_INSTALL_DIR="${FALCONFS_INSTALL_DIR:-/usr/local/falconfs}"
echo "Setting FALCONFS_INSTALL_DIR to ${FALCONFS_INSTALL_DIR}"

if command -v pg_config >/dev/null 2>&1; then
    export PG_BINDIR="${PG_BINDIR:-$(pg_config --bindir)}"
    export PG_LIBDIR="${PG_LIBDIR:-$(pg_config --libdir)}"
    export PG_SHAREDIR="${PG_SHAREDIR:-$(pg_config --sharedir)}"
    export PG_PKGLIBDIR="${PG_PKGLIBDIR:-$(pg_config --pkglibdir)}"
fi

missing_pg_vars=()
[ -z "${PG_BINDIR:-}" ] && missing_pg_vars+=("PG_BINDIR")
[ -z "${PG_LIBDIR:-}" ] && missing_pg_vars+=("PG_LIBDIR")
[ -z "${PG_SHAREDIR:-}" ] && missing_pg_vars+=("PG_SHAREDIR")
[ -z "${PG_PKGLIBDIR:-}" ] && missing_pg_vars+=("PG_PKGLIBDIR")

if [ "${#missing_pg_vars[@]}" -gt 0 ]; then
    echo "Error: PostgreSQL runtime paths are incomplete and pg_config is unavailable." >&2
    echo "Missing: ${missing_pg_vars[*]}" >&2
    echo "Install the PostgreSQL devel package that provides pg_config, or set all of:" >&2
    echo "  export PG_BINDIR=/path/to/postgresql/bin" >&2
    echo "  export PG_LIBDIR=/path/to/postgresql/lib" >&2
    echo "  export PG_SHAREDIR=/path/to/postgresql/share" >&2
    echo "  export PG_PKGLIBDIR=/path/to/postgresql/pkglib" >&2
    return 1 2>/dev/null || exit 1
fi

export CONFIG_FILE="$FALCONFS_INSTALL_DIR/falcon_client/config/config.json"
export PATH="$FALCONFS_INSTALL_DIR/falcon_client/bin:$PG_BINDIR:$PATH"
export LD_LIBRARY_PATH="/usr/local/obs/lib:$PG_LIBDIR:${LD_LIBRARY_PATH:-}"
