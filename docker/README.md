## FalconFS dev machine

- ubuntu 22.04
- FalconFS dev machine version `v0.1.0`

## build and deploy

```bash
docker buildx build --platform linux/amd64,linux/arm64 \
    -t ghcr.io/falcon-infra/falconfs-dev:latest \
    -t ghcr.io/falcon-infra/falconfs-dev:v0.1.0 \
    -f ubuntu22.04-dockerfile \
    . --push
```

## test

```bash
docker run -it --rm -v `pwd`/..:/root/code ghcr.io/falcon-infra/falconfs-dev /bin/zsh
```
