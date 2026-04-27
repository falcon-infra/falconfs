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

# Runtime third-party shared libraries (zookeeper/brpc/etc.) are prepared
# out-of-band under /usr/local for this package flow.
# Disable RPM automatic ELF dependency/provides generation to avoid forcing
# repository-provided soname packages that may not exist on openEuler.
# AutoReqProv:    no

Requires:       bash
Requires:       sudo
Requires:       findutils
Requires:       glibc-all-langpacks
Requires:       python3
Requires:       python3-requests python3-psycopg2 python3-kazoo
Requires:       postgresql-17 postgresql-17-server
Requires:       brpc zookeeper-client-c

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
BuildRequires:  libstdc++-static zstd-devel
BuildRequires:  brpc-devel zookeeper-client-c-devel
BuildRequires:  postgresql-17-server-devel
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
export STAGE_ROOT="%{_builddir}/falconfs-stage"
export FALCONFS_INSTALL_DIR="${STAGE_ROOT}/usr/local/falconfs"
mkdir -p "${STAGE_ROOT}"
echo "Using pg_config: $(command -v pg_config)"
pg_config --pgxs
./build.sh build falcon --comm-plugin=brpc
./build.sh build falcon --comm-plugin=hcom

test -f ./falcon/libbrpcplugin.so
test -f ./falcon/libhcomplugin.so

%install
set -euo pipefail
export STAGE_ROOT="%{_builddir}/falconfs-stage"
export FALCONFS_INSTALL_DIR="${STAGE_ROOT}/usr/local/falconfs"
rm -rf "%{buildroot}" "${STAGE_ROOT}"
mkdir -p "${STAGE_ROOT}"
./build.sh install falcon --comm-plugin=brpc

# Keep both communication plugins in RPM so runtime can switch via config.
install -m 0755 -D ./falcon/libhcomplugin.so \
    "${STAGE_ROOT}/usr/local/falconfs/falcon_meta/lib/postgresql/libhcomplugin.so"

test -f "${STAGE_ROOT}/usr/local/falconfs/falcon_meta/lib/postgresql/libbrpcplugin.so"
test -f "${STAGE_ROOT}/usr/local/falconfs/falcon_meta/lib/postgresql/libhcomplugin.so"
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
mkdir -p "%{buildroot}%{_sysconfdir}/profile.d"
cat > "%{buildroot}%{_sysconfdir}/profile.d/falconfs.sh" <<'EOF'
export FALCONFS_INSTALL_DIR=/usr/local/falconfs
export PATH=/usr/local/falconfs/falcon_client/bin:$PATH
EOF

# Strip non-standard RPATH/RUNPATH to satisfy rpmbuild checks
find "%{buildroot}" -type f -exec chrpath -d {} \; 2>/dev/null || true

%pre
getent group falconMeta >/dev/null || groupadd -r falconMeta
getent passwd falconMeta >/dev/null || \
    useradd -r -g falconMeta -d /home/falconMeta -s /sbin/nologin falconMeta
exit 0

%post
%if 0%{?release_pkg}
/usr/bin/mkdir -p /usr/local/falconfs/data || true
/usr/bin/chown -R falconMeta:falconMeta /usr/local/falconfs/data || true
%endif
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
%{_sysconfdir}/profile.d/falconfs.sh
