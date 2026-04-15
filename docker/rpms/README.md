Place prebuilt dependency RPMs for openEuler release-runtime image here.

Expected examples:
- brpc-*.rpm
- postgresql-17-*.rpm
- postgresql-17-server-*.rpm
- postgresql-17-private-libs-*.rpm
- zookeeper-c-*.rpm

Notes:
- `*debuginfo*` and `*debugsource*` packages are ignored by the Dockerfile.
- Build from repository root so `COPY docker/rpms/*.rpm /tmp/rpms/` works.
