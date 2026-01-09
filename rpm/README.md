# Dependencies

```
yum install -y rpmdevtools
```

# Build falconfs

Build falconfs as well as corresponding dependencies. Notice: the path of third-party libraries should be the same
to FALCON_INSTALL_DIR, otherwise they cann't be found after users installing rpm package.

For example, on OpenEuler 22.03,

```
export FALCONFS_INSTALL_DIR=$HOME/falconfs
bash deploy/ansible/install-openEuler22.03.sh $FALCONFS_INSTALL_DIR
bash build.sh build pg
bash build.sh install pg
bash build.sh build falcon
bash build.sh install falcon
tar -czvf falconfs.tar.gz $FALCONFS_INSTALL_DIR
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