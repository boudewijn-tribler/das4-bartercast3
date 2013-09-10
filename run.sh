#!/bin/bash

module load prun/default

CONFIG="$1"
if [ ! -f "$CONFIG" ]; then
    echo "$0 CONFIGFILE [now|20:00]"
    exit 1
fi

if [ "X$2" == "X" ] || [ "X$2" == "Xnow" ]; then
    PRUNSTART=""
else
    PRUNSTART="-s $2"
fi

source "$CONFIG"

# prepare RESULTDIR
if [ -d "$RESULTDIR" ]; then
    rm -rf "$RESULTDIR" || exit 1
fi
mkdir "$RESULTDIR" || exit 1
touch "$RESULTDIR/shared.db"
cp "$CONFIG" "$RESULTDIR/config" || exit 1

# peek at current tasks
preserve -long-list

# submit tasks
prun -v $PRUNSTART -t "$RUNTIME" -np "$HOSTS" "$SCRIPT" "$CONFIG" 2>&1 | tee "$RESULTDIR/run"

# print -very- small summary.  more processing should be done elsewhere
echo "prun returns $?"
echo "results stored in $RESULTDIR [`cat $RESULTDIR/node*/stderr | wc -l` stderr, `cat $RESULTDIR/node*/stdout | wc -l` stdout]"
