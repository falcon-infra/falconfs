Name:           falconfs
Version:        1.0
Release:        1%{?dist}
Summary:        FalconFS is a high-performance distributed file system (DFS) optimized for AI workloads.

License:        Apache 2.0
URL:            https://gitee.com/openeuler/FalconFS.git
Source0:        falconfs.tar.gz

AutoReq:        0
AutoProv:       0
BuildRequires:  tar
Requires:       bash gcc gcc-c++ glibc-static glibc-devel libmpc-devel tmux flex bison openssl-devel gflags-devel leveldb leveldb-devel glog glog-devel libibverbs libibverbs-utils libibverbs-devel autoconf automake libtool libtool-ltdl-devel cppunit-devel maven java-1.8.0-openjdk-devel ninja-build readline-devel fuse fuse-devel fmt-devel ansible libffi-devel

%description
FalconFS is a high-performance distributed file system (DFS) optimized for AI workloads.

%prep
%setup -q -n falconfs

%global debug_package %{nil}
%global _enable_debug_packages 0
%define __debug_install_post \
%{_rpmconfigdir}/find-debuginfo.sh %{?_find_debuginfo_opts} "%{_builddir}/%{?buildsubdir}"\
%{nil}
%define __arch_install_post %{nil}

%install
mkdir -p %{buildroot}/usr/local/falconfs
cp -a * %{buildroot}/usr/local/falconfs

mkdir -p %{buildroot}/usr/bin
# create soft link

%files
/usr/local/falconfs

%changelog
* Thu Jun 19 2025 root
- 

