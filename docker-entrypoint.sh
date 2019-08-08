#!/bin/bash

if [[ -n $DEV ]] ; then
  python3 -m pip install -e /girderfs
fi

exec python3 -m girder_worker -l INFO $@
