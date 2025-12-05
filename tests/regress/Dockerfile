FROM ubuntu:24.04
RUN apt-get -y update && echo -e "Asia\nShanghai" | apt-get install -y tzdata && \
    apt-get install -y locales && locale-gen en_US.UTF-8

ENV TZ=Asia/Shanghai \
    LANG=en_US.UTF-8 \
    LANGUAGE=en_US:en \
    LC_ALL=en_US.UTF-8

RUN apt-get install -y vim gdb pstack python3 python3-pip bc

RUN mkdir /mnt/data
COPY falconfs /root/falconfs
ENV LD_LIBRARY_PATH=/root/falconfs/lib/
RUN chmod a+x /root/falconfs/docker-entrypoint.sh
ENTRYPOINT ["/root/falconfs/docker-entrypoint.sh"]