#!/usr/bin/env python

import sqlite3
import sys

from dispersy.crypto import ec_generate_key, ec_to_public_bin, ec_to_private_bin

def main():
    if len(sys.argv) == 3:
        database = sqlite3.Connection(sys.argv[1])
        cur = database.cursor()
        count = int(sys.argv[2])

        cur.execute(u"""
CREATE TABLE predefined_identities (
 peer_number INTEGER PRIMARY KEY,
 public_key BLOB,
 private_key BLOB)""")

        for peer_number in xrange(count):
            ec = ec_generate_key(u"NID_secp224r1")
            public_key = ec_to_public_bin(ec)
            private_key = ec_to_private_bin(ec)
            cur.execute(u"INSERT INTO predefined_identities (peer_number, public_key, private_key) VALUES (?, ?, ?)",
                        (peer_number, buffer(public_key), buffer(private_key)))

        database.commit()

    else:
        print sys.argv[0], "DATABASE PEERCOUNT"
        print sys.argv[0], "traces/500peers.db 500"

if __name__ == "__main__":
    main()
