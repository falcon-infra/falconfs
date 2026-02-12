%if 0%{?release_pkg}
Name:           falconfs-release
%else
Name:           falconfs
%endif
Version:        0.1.0
Release:        1%{?dist}
Summary:        FalconFS is a high-performance distributed file system (DFS) optimized for AI workloads.

License:        Apache-2.0
URL:            https://gitee.com/openeuler/FalconFS.git
Source0:        falconfs-%{version}.tar.gz

BuildRequires:  bash gcc gcc-c++ make
BuildRequires:  cmake ninja-build
BuildRequires:  autoconf automake libtool
BuildRequires:  bison flex readline-devel
BuildRequires:  openssl-devel
BuildRequires:  gflags-devel glog-devel leveldb-devel snappy-devel
BuildRequires:  fmt-devel
BuildRequires:  gperftools-devel libunwind-devel
BuildRequires:  rdma-core-devel
BuildRequires:  fuse-devel
BuildRequires:  libcurl-devel
BuildRequires:  jansson-devel libffi-devel
BuildRequires:  libzstd-devel xz-devel expat-devel
BuildRequires:  libxml2-devel
BuildRequires:  systemd-devel
BuildRequires:  protobuf-devel protobuf-compiler
BuildRequires:  flatbuffers-devel flatbuffers-compiler
BuildRequires:  jsoncpp-devel thrift-devel
BuildRequires:  cppunit-devel
BuildRequires:  gtest-devel gmock-devel
BuildRequires:  python3-devel
BuildRequires:  maven java-11-openjdk-devel
BuildRequires:  wget tar rsync
BuildRequires:  libstdc++-static libpq-devel zstd-devel
BuildRequires:  perl
BuildRequires:  chrpath
BuildRequires:  python3-requests python3-psycopg2 python3-kazoo


%description
FalconFS is a high-performance distributed file system.
It integrates seamlessly with cloud environments.

%global _debugsource_packages 0
%global debug_package %{nil}

%prep
%if 0%{?release_pkg}
%setup -q -n falconfs-%{version}
%else
%setup -q
%endif

%build
set -euo pipefail
export PATH=/usr/local/pgsql/bin:$PATH
export STAGE_ROOT="%{_builddir}/falconfs-stage"
export FALCONFS_INSTALL_DIR="${STAGE_ROOT}/usr/local/falconfs"
mkdir -p "${STAGE_ROOT}"
echo "Using pg_config: $(command -v pg_config)"
pg_config --pgxs
./build.sh build falcon

%install
set -euo pipefail
export PATH=/usr/local/pgsql/bin:$PATH
export STAGE_ROOT="%{_builddir}/falconfs-stage"
export FALCONFS_INSTALL_DIR="${STAGE_ROOT}/usr/local/falconfs"
rm -rf "%{buildroot}" "${STAGE_ROOT}"
mkdir -p "${STAGE_ROOT}"
./build.sh install falcon
mkdir -p "%{buildroot}"
cp -a "${STAGE_ROOT}/"* "%{buildroot}/"
%if 0%{?release_pkg}
rm -rf "%{buildroot}/usr/local/falconfs/deploy"
rm -rf "%{buildroot}/usr/local/falconfs/falcon_client"
rm -rf "%{buildroot}/usr/local/falconfs/falcon_python_interface"
rm -rf "%{buildroot}/usr/local/falconfs/falcon_regress"
rm -rf "%{buildroot}/usr/local/falconfs/falcon_store"
rm -rf "%{buildroot}/usr/local/falconfs/private-directory-test"
%endif
mkdir -p "%{buildroot}/usr/local/obs"
if [ -d "/usr/local/obs" ]; then
    mkdir -p "%{buildroot}/usr/local"
    cp -a "/usr/local/obs" "%{buildroot}/usr/local/"
fi

# Remove copied system runtime libraries from private FalconFS lib dirs.
# Keep only private/non-system dependencies bundled by FalconFS.
for lib_dir in \
    "%{buildroot}/usr/local/falconfs/falcon_meta/lib" \
    "%{buildroot}/usr/local/falconfs/falcon_client/lib" \
    "%{buildroot}/usr/local/falconfs/private-directory-test/lib"; do
    if [ -d "${lib_dir}" ]; then
        find "${lib_dir}" -maxdepth 1 \
            \( -name 'libc.so*' -o -name 'libm.so*' -o -name 'libpthread.so*' -o \
               -name 'libdl.so*' -o -name 'librt.so*' -o -name 'libresolv.so*' -o \
               -name 'libcrypt.so*' -o -name 'ld-linux*.so*' \) \
            -print -delete
    fi
done

mkdir -p "%{buildroot}%{_sysconfdir}/profile.d"
cat > "%{buildroot}%{_sysconfdir}/profile.d/falconfs.sh" <<'EOF'
export FALCONFS_INSTALL_DIR=/usr/local/falconfs
export PATH=/usr/local/pgsql/bin:/usr/local/falconfs/falcon_client/bin:$PATH
export LD_LIBRARY_PATH=/usr/local/falconfs/falcon_meta/lib:/usr/local/falconfs/falcon_client/lib:/usr/local/obs/lib:$LD_LIBRARY_PATH
export FALCONFS_WORKSPACE=/var/lib/falconfs
export PGUSER=falconMeta
EOF

# Strip non-standard RPATH/RUNPATH to satisfy rpmbuild checks
find "%{buildroot}" -type f -exec chrpath -d {} \; 2>/dev/null || true

%pre
getent group falconMeta >/dev/null || groupadd -r falconMeta
getent passwd falconMeta >/dev/null || \
    useradd -r -g falconMeta -d /home/falconMeta -s /sbin/nologin falconMeta
exit 0

%post
/usr/bin/mkdir -p /var/lib/falconfs/data || true
/usr/bin/chown -R falconMeta:falconMeta /var/lib/falconfs/data || true
/usr/sbin/usermod -d /home/falconMeta falconMeta || true
/usr/bin/mkdir -p /home/falconMeta || true
/usr/bin/chown -R falconMeta:falconMeta /home/falconMeta || true

%postun
:

%files
%if 0%{?release_pkg}
%dir /usr/local/falconfs
/usr/local/falconfs/falcon_meta/
/usr/local/falconfs/falcon_cm/
/usr/local/falconfs/falcon_cn/
/usr/local/falconfs/falcon_dn/
%else
/usr/local/falconfs/
%endif
/usr/local/obs/
%{_sysconfdir}/profile.d/falconfs.sh
