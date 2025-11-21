# Dependencies

```
yum install -y rpmdevtools
```

# Create RPM package

```
rpmdev-setuptree
cp falconfs.spec ~/rpmbuild/SPECS/
cp falconfs.tar.gz ~/rpmbuild/SOURCES/
rpmbuild -ba ~/rpmbuild/SPECS/falconfs.spec
ls -alh ~/rpmbuild/RPMS/aarch64/falconfs-1.0-1.aarch64.rpm
```

# Install
```
yum localinstall falconfs-1.0-1.aarch64.rpm
```