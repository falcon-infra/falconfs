## FalconFS openEuler dev machine

- openEuler 24.03 LTS
- GCC 14.2.0 (C++23 support)
- FalconFS dev machine for openEuler

## build

```bash
docker build -f docker/openeuler24.03-dockerfile -t falconfs-openeuler:24.03-lts docker/
```

## test

```bash
docker run -it --privileged --rm -v `pwd`:/root/code -w /root/code/falconfs falconfs-openeuler:24.03-lts /bin/bash
```

## deploy

```bash
docker tag falconfs-openeuler:24.03-lts your-registry/falconfs-openeuler:24.03-lts
docker push your-registry/falconfs-openeuler:24.03-lts
```