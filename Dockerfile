FROM xarthisius/girder_worker:latest

ENV C_FORCE_ROOT=1 

USER root

RUN apt-get update -qy && \
  apt-get -qy install vim git fuse libfuse-dev && \
  apt-get -qy clean all && \
  echo "user_allow_other" >> /etc/fuse.conf && \
  rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN girder-worker-config set celery backend redis://redis/ && \
  girder-worker-config set celery broker redis://redis/

COPY requirements.txt /gwvolman/requirements.txt
COPY setup.py /gwvolman/setup.py
COPY gwvolman /gwvolman/gwvolman

WORKDIR /gwvolman
RUN pip install -r requirements.txt -e . && rm -rf /tmp/*

COPY mount.c /tmp/mount.c
RUN gcc -Wall -fPIC -shared -o /usr/local/lib/container_mount.so /tmp/mount.c -ldl -D_FILE_OFFSET_BITS=64 && \
   rm  /tmp/mount.c && \
   chmod +x /usr/local/lib/container_mount.so && \
   echo "/usr/local/lib/container_mount.so" > /etc/ld.so.preload
