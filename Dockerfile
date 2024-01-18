FROM ubuntu:jammy

RUN apt-get update -qqy && \
  apt-get install -qy software-properties-common python3-software-properties && \
  DEBIAN_FRONTEND=noninteractive apt-get -qy install \
    build-essential \
    tini \
    vim \
    git \
    gosu \
    wget \
    python3 \
    python3-pip \
    python3-venv \
    fuse \
    davfs2 \
    libmagic1 \
    libffi-dev \
    libssl-dev \
    libjpeg-dev \
    libcurl4-openssl-dev \
    zlib1g-dev \
    libfuse-dev \
    moreutils \
    sudo \
    libpython3-dev && \
  apt-get -qqy clean all && \
  echo "user_allow_other" >> /etc/fuse.conf && \
  rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN groupadd -g 1000 wtgroup && useradd -g 1000 -G 1000 -u 1000 -m -s /bin/bash wtuser
RUN echo "source /home/wtuser/venv/bin/activate" >> /etc/bash.bashrc
RUN echo "wtuser ALL=(ALL)    NOPASSWD: /usr/bin/mount, /usr/bin/umount" >> /etc/sudoers

USER wtuser
WORKDIR /gwvolman

COPY --chown=wtuser:wtgroup requirements.txt /gwvolman/requirements.txt
COPY --chown=wtuser:wtgroup setup.py /gwvolman/setup.py
COPY --chown=wtuser:wtgroup gwvolman /gwvolman/gwvolman

RUN python3 -m venv /home/wtuser/venv
RUN . /home/wtuser/venv/bin/activate \
  && pip install -U setuptools wheel \
  && pip install --no-cache-dir -r requirements.txt -e . \
  && rm -rf /tmp/*

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

# Temporary fix for girder_utils (chain tasks and kwargs)
RUN sed \
  -e "/serializer/ s/girder_io/json/" \
  -i /home/wtuser/venv/lib/python3.10/site-packages/girder_worker/task.py

USER root
# https://github.com/whole-tale/gwvolman/issues/51
# https://github.com/whole-tale/wt_home_dirs/issues/18
RUN echo "use_locks 0" >> /etc/davfs2/davfs2.conf && \
  echo "backup_dir .lost+found" >> /etc/davfs2/davfs2.conf && \
  echo "delay_upload 1" >> /etc/davfs2/davfs2.conf && \
  echo "gui_optimize 1" >> /etc/davfs2/davfs2.conf

COPY docker-entrypoint.sh /docker-entrypoint.sh
COPY scheduler-entrypoint.sh /scheduler-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh
RUN chmod +x /scheduler-entrypoint.sh
ENTRYPOINT ["/usr/bin/tini", "--", "/docker-entrypoint.sh"]
