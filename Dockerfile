FROM ubuntu:xenial

RUN apt-get update -qqy && \
  apt-get install -qy software-properties-common python3-software-properties && \
  DEBIAN_FRONTEND=noninteractive apt-get -qy install \
    build-essential \
    vim \
    git \
    wget \
    python3 \
    fuse \
    davfs2 \
    libffi-dev \
    libssl-dev \
    libjpeg-dev \
    zlib1g-dev \
    libfuse-dev \
    libpython3-dev && \
  apt-get -qqy clean all && \
  echo "user_allow_other" >> /etc/fuse.conf && \
  rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN wget https://bootstrap.pypa.io/get-pip.py && python3 get-pip.py

COPY requirements.txt /gwvolman/requirements.txt
COPY setup.py /gwvolman/setup.py
COPY gwvolman /gwvolman/gwvolman

WORKDIR /gwvolman
RUN LDFLAGS="-Wl,-rpath='/usr/local/lib',--enable-new-dtags $LDFLAGS" pip install --no-cache-dir -r requirements.txt -e . && rm -rf /tmp/*

COPY mount.c /tmp/mount.c
RUN gcc -Wall -fPIC -shared -o /usr/local/lib/container_mount.so /tmp/mount.c -ldl -D_FILE_OFFSET_BITS=64 && \
   rm  /tmp/mount.c && \
   chmod +x /usr/local/lib/container_mount.so && \
   echo "/usr/local/lib/container_mount.so" > /etc/ld.so.preload

RUN useradd -g 100 -G 100 -u 1000 -s /bin/bash wtuser

RUN girder-worker-config set celery backend redis://redis/ && \
  girder-worker-config set celery broker redis://redis/ && \
  girder-worker-config set girder_worker tmp_root /tmp

ENV C_FORCE_ROOT=1
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

# Temporary fix for kombu
RUN sed \
  -e 's/return decode(data/&.decode("utf-8")/' \
  -i /usr/local/lib/python3.5/dist-packages/kombu/serialization.py

# Temporary fix for girder_utils (chain tasks and kwargs)
RUN sed \
  -e "/'kwargs':/ s/task_kwargs/json.dumps(&)/" \
  -i /usr/local/lib/python3.5/dist-packages/girder_worker/context/nongirder_context.py

# https://github.com/whole-tale/gwvolman/issues/51
# https://github.com/whole-tale/wt_home_dirs/issues/18
RUN echo "use_locks 0" >> /etc/davfs2/davfs2.conf && \
  echo "backup_dir .lost+found" >> /etc/davfs2/davfs2.conf && \
  echo "gui_optimize 1" >> /etc/davfs2/davfs2.conf

COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh
ENTRYPOINT ["/docker-entrypoint.sh"]
