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

RUN cd /tmp && \
  wget https://raw.githubusercontent.com/whole-tale/gwvolman/wthomedir-dev/requirements.txt && \
  pip install -r requirements.txt && rm -rf /tmp/*

RUN pip install git+https://github.com/whole-tale/gwvolman.git@wthomedir-dev

COPY mount.c /tmp/mount.c
RUN gcc -Wall -fPIC -shared -o /usr/local/lib/container_mount.so /tmp/mount.c -ldl -D_FILE_OFFSET_BITS=64 && \
   rm  /tmp/mount.c && \
   chmod +x /usr/local/lib/container_mount.so && \
   echo "/usr/local/lib/container_mount.so" > /etc/ld.so.preload
