## FalconFS dev machine

- ubuntu 24.04
- FalconFS dev machine version `v0.2.0-ubuntu24.04`

## build and deploy

ubuntu 22.04
```bash
docker buildx build --platform linux/amd64,linux/arm64 \
    -t ghcr.io/falcon-infra/falconfs-dev:ubuntu22.04 \
    -t ghcr.io/falcon-infra/falconfs-dev:v0.1.0-ubuntu22.04 \
    -f ubuntu22.04-dev-dockerfile \
    . --push
```

ubuntu 24.04
```bash
docker buildx build --platform linux/amd64,linux/arm64 \
    -t ghcr.io/falcon-infra/falconfs-dev:ubuntu24.04 \
    -t ghcr.io/falcon-infra/falconfs-dev:v0.2.0-ubuntu24.04 \
    -t ghcr.io/falcon-infra/falconfs-dev:latest \
    -f ubuntu24.04-dev-dockerfile \
    . --push
```

## test

```bash
docker run -it --rm -v `pwd`/..:/root/code ghcr.io/falcon-infra/falconfs-dev /bin/zsh
```

## build and depoly cn, dn, store

current falconfs verstion `v0.1.0-alpha` 

### base builder

``` bash
cd ~/code/falconfs/docker
```

```bash
docker buildx build --build-arg FALCONFS_TAG=v0.1.0-alpha --platform linux/amd64,linux/arm64 \
    -t ghcr.io/falcon-infra/falconfs-base-builder:ubuntu24.04 \
    -t ghcr.io/falcon-infra/falconfs-base-builder:v0.1.0-alpha-ubuntu24.04 \
    -t ghcr.io/falcon-infra/falconfs-base-builder:latest \
    -f ubuntu24.04-base-builder-dockerfile \
    . \
    --push
```

### cn
```bash
docker buildx build --platform linux/amd64,linux/arm64 \
    -t ghcr.io/falcon-infra/falconfs-cn:ubuntu24.04 \
    -t ghcr.io/falcon-infra/falconfs-cn:v0.1.0-alpha-ubuntu24.04 \
    -f ubuntu24.04-cn-dockerfile \
    . \
    --push
```
