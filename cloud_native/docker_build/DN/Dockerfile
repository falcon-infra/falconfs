FROM ubuntu:24.04
RUN apt-get -y update && echo -e "Asia\nShanghai" | apt-get install -y tzdata && \
    apt-get install -y locales && locale-gen en_US.UTF-8

ENV TZ=Asia/Shanghai \
    LANG=en_US.UTF-8 \
    LANGUAGE=en_US:en \
    LC_ALL=en_US.UTF-8
RUN apt-get install -y gdb vim tar python3 python3-pip libreadline-dev liblz4-dev libzstd-dev libssl-dev
RUN mkdir /opt/falcon && chmod -R 0755 /opt/falcon
ARG USER_NAME=falconMeta
RUN useradd -m -s /bin/bash $USER_NAME
USER $USER_NAME
COPY --chown=$USER_NAME:$USER_NAME docker-entrypoint.sh /home/falconMeta
COPY --chown=$USER_NAME:$USER_NAME postgresql_falcon.conf /home/falconMeta
COPY --chown=$USER_NAME:$USER_NAME falcon_cm /home/falconMeta/falcon_cm
COPY --chown=$USER_NAME:$USER_NAME metadb /home/falconMeta/metadb
COPY --chown=$USER_NAME:$USER_NAME start.sh /home/falconMeta
COPY --chown=$USER_NAME:$USER_NAME stop.sh /home/falconMeta
COPY --chown=$USER_NAME:$USER_NAME check_liveness.sh /home/falconMeta
COPY --chown=$USER_NAME:$USER_NAME rm_logs.sh /home/falconMeta
RUN mkdir /home/falconMeta/readiness/ && mkdir /home/falconMeta/data/
ENV PATH=/home/falconMeta/metadb/bin/:$PATH \
    LD_LIBRARY_PATH=/home/falconMeta/metadb/lib/ \
    TMOUT=0 
RUN echo PATH=$PATH >> ~/.bashrc
RUN pip3 install --break-system-packages psycopg2 && pip3 install --break-system-packages kazoo
RUN chmod +x /home/falconMeta/docker-entrypoint.sh
USER root
EXPOSE 5432
ENTRYPOINT ["/home/falconMeta/docker-entrypoint.sh"]
