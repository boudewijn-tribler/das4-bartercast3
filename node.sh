#!/bin/bash

(
    flock -e 200

    # $HOSTNAME fails, obtain from /etc/HOSTNAME
    HOSTNAME=`cat /etc/HOSTNAME`

    # determine the node index
    RANK=$PRUN_CPU_RANK

    #
    # 1 - parameters
    #

    CONFIG="$1"
    if [ ! -f "$CONFIG" ]; then
        echo "`date` ${HOSTNAME} error: $0 CONFIGFILE"
        exit 1
    fi

    #
    # 2 - environment
    #

    # prepare local working directory
    if [ -d "/local/$USER" ]; then
        echo "`date` ${HOSTNAME} warning: local workdir already exists (removing...)"
        rm -rf "/local/$USER"
    fi
    mkdir "/local/$USER"

    LOCALCONFIG="/local/$USER/config"
    cp -r "$CONFIG" "$LOCALCONFIG"
    source "$LOCALCONFIG"

    if [ ! "$SGE_KEEP_TMPFILES" == "no" ]; then
        echo "`date` ${HOSTNAME} error: SGE_KEEP_TMPFILES should be 'no'"
        echo "`date` ${HOSTNAME} error: this property must be set in .bashrc [${HOME}/.bashrc]"
        exit 2
    fi

    # start of runtime
    STARTSTAMP=`date +%s`

    #
    # 3 - setup peer
    #

    # copy python branch to the local disk
    LOCALCODEDIR="/local/$USER/localcodedir"
    cp -r "$PYTHONCODEDIR" "$LOCALCODEDIR"
    export PYTHONPATH="$LOCALCODEDIR:$PYTHONPATH"

    # calculate the PEERNUMBER range that this node is responsible for
    LOWPEERNUMBER=`python -c "print $TASKS / $HOSTS * $RANK + min($TASKS % $HOSTS, $RANK)"`
    HIGHPEERNUMBER=`python -c "print $TASKS / $HOSTS * ($RANK + 1) + min($TASKS % $HOSTS, $RANK + 1) - 1"`
    echo "= `date` ${HOSTNAME} start [$LOWPEERNUMBER:$HIGHPEERNUMBER]"

    for (( BATCHNUMBER=LOWPEERNUMBER; BATCHNUMBER<=HIGHPEERNUMBER; BATCHNUMBER+=BATCHSIZE )); do
        for (( PEERNUMBER=BATCHNUMBER; PEERNUMBER<BATCHNUMBER+BATCHSIZE && PEERNUMBER<=HIGHPEERNUMBER; PEERNUMBER++ )); do

            PEER_WORKDIR="/local/${USER}/${HOSTNAME}_${PEERNUMBER}"
            if [ -d "$PEER_WORKDIR" ]; then
                echo "`date` ${HOSTNAME} error: PEER_WORKDIR already exists [$PEER_WORKDIR]"
                exit 3
            fi

            mkdir "$PEER_WORKDIR"
            cd "$PEER_WORKDIR"
            if [ -f "$LOGGERCONF" ]; then
                cp "$LOGGERCONF" logger.conf
            fi
            if [ -f "$ZEROLOGGERCONF" ] && [ "$PEERNUMBER" == "0" ]; then
                cp "$ZEROLOGGERCONF" logger.conf
            fi
            if [ -f "$BOOTSTRAPFILE" ]; then
                cp "$BOOTSTRAPFILE" bootstraptribler.txt
            fi

            # output to stdout and stderr files
            # $BINARY $BINARYPARAMS "$LOCALCODEDIR/peer.py" $DISPERSYPARAMS --kargs "${DISPERSYKARGS},resultdir=${RESULTDIR},localcodedir=${LOCALCODEDIR},startstamp=${STARTSTAMP},scenario=${LOCALCONFIG},peernumber=${PEERNUMBER},peercount=${TASKS},lowpeernumber=${LOWPEERNUMBER},highpeernumber=${HIGHPEERNUMBER}" >stdout 2>stderr &

            # output to stdout, stderr files, and console
            # $BINARY $BINARYPARAMS "$LOCALCODEDIR/peer.py" $DISPERSYPARAMS --kargs "${DISPERSYKARGS},resultdir=${RESULTDIR},localcodedir=${LOCALCODEDIR},startstamp=${STARTSTAMP},scenario=${LOCALCONFIG},peernumber=${PEERNUMBER},peercount=${TASKS},lowpeernumber=${LOWPEERNUMBER},highpeernumber=${HIGHPEERNUMBER}" > >(tee stdout) 2> >(tee stderr >&2) &

            # output to stdout, stderr files, and stderr to console
            $BINARY $BINARYPARAMS "$LOCALCODEDIR/peer.py" $DISPERSYPARAMS --log-identifier "${HOSTNAME}_${PEERNUMBER}" --kargs "${DISPERSYKARGS},resultdir=${RESULTDIR},localcodedir=${LOCALCODEDIR},startstamp=${STARTSTAMP},scenario=${LOCALCONFIG},peernumber=${PEERNUMBER},peercount=${TASKS},lowpeernumber=${LOWPEERNUMBER},highpeernumber=${HIGHPEERNUMBER}" >stdout 2> >(tee stderr >&2) &
        done

        # batch delay
        sleep $BATCHDELAY
    done

    # wait for all processes to finish
    echo "= `date` ${HOSTNAME} wait [$LOWPEERNUMBER:$HIGHPEERNUMBER]"
    wait
    echo "= `date` ${HOSTNAME} copy [$LOWPEERNUMBER:$HIGHPEERNUMBER]"

    # remove local branch and scenario
    rm -rf "$LOCALCONFIG" "$LOCALCODEDIR"

    # copy results (note that the '/' behind ${USER} ensures the content of that directory is copied)
    rsync $RSYNCPARAMS --archive "/local/${USER}/" "${USER}@fs3:${RESULTDIR}"

    # done
    echo "= `date` ${HOSTNAME} done [$LOWPEERNUMBER:$HIGHPEERNUMBER]"

) 200>/local/lockfile.$USER
