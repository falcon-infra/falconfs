FROM ubuntu:24.04
RUN apt-get -y update && echo -e "Asia\nShanghai" | apt-get install -y tzdata && \
    apt-get install -y locales && locale-gen en_US.UTF-8

ENV TZ=Asia/Shanghai \
    LANG=en_US.UTF-8 \
    LANGUAGE=en_US:en \
    LC_ALL=en_US.UTF-8

RUN apt-get install -y vim

RUN mkdir /opt/log/ && mkdir /opt/falcon/ && mkdir /opt/conf/ && mkdir /opt/output/ && mkdir /mnt/falcon/
COPY config.json /opt/conf/
ENV CONFIG_FILE=/opt/conf/config.json
COPY falconfs /root/falconfs
ENV LD_LIBRARY_PATH=/root/falconfs/lib/
COPY start.sh /root/
COPY stop.sh /root/
COPY check_liveness.sh /root/

EXPOSE 50039
COPY docker-entrypoint.sh /root/
RUN chmod a+x /root/docker-entrypoint.sh
ENTRYPOINT ["/root/docker-entrypoint.sh"]