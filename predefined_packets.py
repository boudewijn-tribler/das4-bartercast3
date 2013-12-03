#!/usr/bin/env python

from struct import pack
from hashlib import sha1
import sqlite3
import sys

from dispersy.crypto import ec_from_private_bin, ec_sign

def generate_packets(db, cur, cid):
    """
    Reads the predefined_identities and predefined_records tables, generates binary barter records, and writes those
    packets into the predefined_records table.
    """
    def encode(peer1, peer2, global_time, cycle, up122, up221):
        #assert peer1 < peer2, [peer1, peer2]
        key1, ec1 = ecs[peer1]
        key2, ec2 = ecs[peer2]
        body = "".join(("\x00\x01",     # versions
                        cid,            # community identifier
                        chr(1),         # message type

                        # DoubleMemberAuthentication
                        pack(">HH", len(key1), len(key2)),
                        key1, key2,     # public keys

                        # PublicResolution
                        # -- no fields

                        # LastSyncDistribution
                        pack(">Q", global_time),

                        # CommunityDestination
                        # -- no fields

                        # BarterRecordPayload
                        pack(">LQQB",
                             long(cycle),       # cycle
                             long(up122),       # upload first to second
                             long(up221),       # upload second to first
                             1),                # length of effort bytes
                        "\x00",                 # effort bytes
                        pack(">LLQQQQ",
                             1,                 # first timestamp
                             1,                 # second timestamp
                             0,                 # first upload
                             0,                 # first download
                             0,                 # second upload
                             0),                # second download
                        ))

        # add signatures
        digest = sha1(body).digest()
        packet = "".join((body,
                          ec_sign(ec1, digest),
                          ec_sign(ec2, digest)))

        return (buffer(packet), peer1, peer2)

    # we must have a fixed community id
    print "# generating packets for community hash:", cid.encode("HEX")

    ecs = dict((peer_number, (str(public_key), ec_from_private_bin(str(private_key))))
               for peer_number, public_key, private_key
               in cur.execute(u"SELECT peer_number, public_key, private_key FROM predefined_identities"))

    print "# reading records"
    records = list(cur.execute(u"SELECT first_peer_number, second_peer_number, global_time, cycle, upload_first_to_second, upload_second_to_first FROM predefined_records"))

    print "# writing", len(records), "packets"
    for args in records:
        cur.execute(u"UPDATE predefined_records SET packet = ? WHERE first_peer_number = ? AND second_peer_number = ?", encode(*args))

def main():
    if len(sys.argv) == 3 and len(sys.argv[2]) == 40:
        database = sqlite3.Connection(sys.argv[1])
        cur = database.cursor()
        cid = sys.argv[2]

        try:
            cur.execute(u"ALTER TABLE predefined_records ADD COLUMN packet BLOB")
        except Exception as exception:
            if exception.message != "duplicate column name: packet":
                raise

        generate_packets(database, cur, cid.decode("HEX"))
        database.commit()

    else:
        print sys.argv[0], "DATABASE", "4d854cbc14f36f6ef9411a689fc90ce7679f210a"
        print sys.argv[0], "traces/500peers.db CID"

if __name__ == "__main__":
    main()
