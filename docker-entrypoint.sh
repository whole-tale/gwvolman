#!/bin/bash

if [[ -n $DEV ]] ; then
  python3 -m pip install -e /girderfs
  python3 -m pip install -r /gwvolman/requirements.txt -e /gwvolman
fi

exec python3 -m girder_worker -l INFO $@
