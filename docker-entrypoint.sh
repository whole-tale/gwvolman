#!/bin/bash

set -e
. /home/ubuntu/venv/bin/activate
girder-worker-config set celery backend ${CELERY_BACKEND:-redis://redis/}
girder-worker-config set celery broker ${CELERY_BROKER:-redis://redis/}
girder-worker-config set girder_worker tmp_root /tmp

if [[ -n "$DEV" ]] ; then
  python3 -m pip install -e /gwvolman
fi

# If GOSU_CHOWN environment variable set, recursively chown all specified directories
# to match the user:group set in GOSU_USER environment variable.
if [ -n "$GOSU_CHOWN" ]; then
    for DIR in $GOSU_CHOWN
    do
        chown -R $GOSU_UID:$GOSU_GID $DIR
    done
fi

# If GOSU_USER environment variable set to something other than 0:0 (root:root),
# become user:group set within and exec command passed in args
if [ "$GOSU_USER" != "0:0" ]; then
    IFS=: read GOSU_UID GOSU_GID DOCKER_GROUP <<<"${GOSU_USER}"
    if [ -z $(getent group $DOCKER_GROUP) ] ; then
      groupadd -g $DOCKER_GROUP docker
    fi
    gpasswd -a ubuntu $(getent group $DOCKER_GROUP | cut -f1 -d:)
    usermod -g $GOSU_GID ubuntu
    exec gosu $GOSU_UID celery -A girder_worker.app worker -l INFO "$@"
fi

# If GOSU_USER was 0:0 exec command passed in args without gosu (assume already root)
exec celery -A girder_worker.app worker -l INFO "$@"
