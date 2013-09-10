#!/usr/bin/env python

import sqlite3
import sys
import random
import itertools
from scipy.stats import powerlaw

def gen_random(db, cur, pedge, min_global_time, max_global_time, min_cycle, max_cycle, min_bytes, max_bytes):
    pedge = float(pedge)
    min_global_time = int(min_global_time)
    max_global_time = int(max_global_time)
    min_cycle = int(min_cycle)
    max_cycle = int(max_cycle)
    min_bytes = int(min_bytes)
    max_bytes = int(max_bytes)
    peers = [peer_number for peer_number, in cur.execute(u"SELECT peer_number FROM predefined_identities ORDER BY peer_number")]
    numargs = powerlaw.numargs
    [a] = [0.9,] * numargs

    for first_peer_number, second_peer_number in itertools.combinations(peers, 2):
        assert first_peer_number < second_peer_number, [first_peer_number, second_peer_number]
        if random.random() < pedge:
            global_time = random.randint(min_global_time, max_global_time)
            cycle = random.randint(min_cycle, max_cycle)
            upload_first_to_second = powerlaw.rvs(a)*max_bytes #random.randint(min_bytes, max_bytes)
            upload_second_to_first = powerlaw.rvs(a)*max_bytes #random.randint(min_bytes, max_bytes)
            cur.execute(u"INSERT INTO predefined_records (first_peer_number, second_peer_number, global_time, cycle, upload_first_to_second, upload_second_to_first) VALUES (?, ?, ?, ?, ?, ?)",
                        (first_peer_number, second_peer_number, global_time, cycle, upload_first_to_second, upload_second_to_first))

def main():
    mapping = {"random":gen_random} #,"power_law":gen_power_law}

    if len(sys.argv) >= 3 and sys.argv[2] in mapping:
        database = sqlite3.Connection(sys.argv[1])
        cur = database.cursor()

        cur.execute(u"""
CREATE TABLE predefined_records (
 first_peer_number INTEGER,
 second_peer_number INTEGER,
 global_time INTEGER,
 cycle INTEGER,
 effort BLOB,
 upload_first_to_second INTEGER,
 upload_second_to_first INTEGER,
 packet BLOB,
 PRIMARY KEY (first_peer_number, second_peer_number))""")

        mapping[sys.argv[2]](database, cur, *sys.argv[3:])
        database.commit()

    else:
        print sys.argv[0], "DATABASE random PEDGE MINGLOBALTIME MAXGLOBALTIME MINCYCLE MAXCYCLE MINBYTES MAXBYTES"
        print sys.argv[0], "traces/500peers.db random 0.1 666 6666 999 9999 1024 90*pow(2,20)"

if __name__ == "__main__":
    main()
