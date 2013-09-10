#!/usr/bin/env bash                                                                                                                                                                                                                                                                                                            
  
# directory with all the scripts                                                                                                                                                                                                                                                                                               
EVAL=`dirname $0`

# ARGUMENT $1: directory with all the logs, default '.'
RESULTDIR=${1:-.}

# source config file
if [ ! -f "${RESULTDIR}/config" ]; then
    echo "${RESULTDIR} does not contain config file"
    exit 1
fi
source "${RESULTDIR}/config"

# ARGUMENT $2: database filename, default 'try.db'
DATABASE=${2:-${FILENAME_PREFIX}try.db}
if [ -f "${DATABASE}" ]; then
    echo "${DATABASE} already exists, please remove it if you want to re-evaluate all logs"
    exit 1
fi

# echo on
set -o verbose

# run all scripts                                                                                                                                                                                                                                                                                                              
$EVAL/11-parse.py $RESULTDIR log $DATABASE || exit 1
cat $EVAL/21-graphs.R | sed s:==FILENAME==:$DATABASE: | R --no-save --quiet || echo "FAIL GRAPHS... CONTINUE"
cat $EVAL/22-experiment-graphs.R | sed s:==FILENAME==:$DATABASE: | R --no-save --quiet || echo "FAIL GRAPHS... CONTINUE"
sqlite3 -separator ' ' $DATABASE "select r.first, r.second FROM last_record l JOIN record r ON r.id = l.record" > edges.txt
cat $EVAL/42-edges-graph.R | R --no-save --quiet || echo "FAIL GRAPHS... CONTINUE"

# get stats from database
sqlite3 -header -separator ' ' $DATABASE "SELECT peer AS source, destination_peer AS target, count(*) AS weight FROM walk_candidate GROUP BY peer, destination_peer" > walks.txt
sqlite3 -header -separator ' ' $DATABASE "SELECT p.peer, p.timestamp AS when_received, p.timestamp AS when_created, r.first, r.second, r.global_time, r.cycle, r.upload_first_to_second, r.upload_second_to_first, r.avg_timestamp FROM received_record p JOIN record r ON r.id = p.record ORDER BY p.peer" > received_record.txt
