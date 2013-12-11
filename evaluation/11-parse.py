#!/usr/bin/env python

import sqlite3
import sys

from dispersy.tool.scenarioscript import ScenarioParser1, ScenarioParser2

class BarterScenarioParser(ScenarioParser2):
    """
    Parses log entries called barter-record.
    """
    def __init__(self, database):
        super(BarterScenarioParser, self).__init__(database)

        # LAST_RECORD contains (first, second):global_time pairs
        self.last_record = {}

        # self.cur.execute(u"CREATE TABLE peer (id INTEGER PRIMARY KEY AUTOINCREMENT, peer TEXT, class TEXT);")
        # self.cur.execute(u"ALTER TABLE peer ADD COLUMN class TEXT")
        # self.cur.execute(u"ALTER TABLE peer ADD COLUMN malicious TEXT")
        self.cur.executescript(u"""
-- record contains every record that is created during the emulation
CREATE TABLE record (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 first INTEGER,
 second INTEGER,
 global_time INTEGER,
 cycle INTEGER,
 effort BLOB,
 upload_first_to_second INTEGER,
 upload_second_to_first INTEGER,
 avg_timestamp FLOAT,
 UNIQUE (first, second, global_time));

-- last_record links all records that have not been updated/replaced
CREATE TABLE last_record (record INTEGER PRIMARY KEY REFERENCES record(id));

-- received_record links all records that each individual peer has updated/received
CREATE TABLE received_record (
 record INTEGER,                        -- the rowid of the received record
 peer INTEGER,                          -- the rowid of the peer who received the record
 timestamp FLOAT,                       -- when the peer received the record
 walk INTEGER,                          -- the rowid of the most recent walk that the peer did
 PRIMARY KEY (record, peer));

-- every candidate contacted during walk
CREATE TABLE walk_candidate (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 peer INTEGER,                          -- the peer that initiated contact
 destination_peer INTEGER,              -- the peer that is contacted
 timestamp FLOAT,                       -- when the contact occurred
 timestep INTEGER                       -- the nth walk performed by the peer
);
""")

        # self.cur.execute(u"CREATE TABLE edge (id INTEGER PRIMARY KEY AUTOINCREMENT, cycle INTEGER, peer INTEGER, peer1 INTEGER, peer2 INTEGER)")
        # self.cur.execute(u"CREATE INDEX edge_index_0 ON edge (peer)")
        # self.cur.execute(u"CREATE TABLE online (id INTEGER PRIMARY KEY AUTOINCREMENT, cycle INTEGER, peer INTEGER)")
        # self.cur.execute(u"CREATE TABLE signature (peer INTEGER, request INTEGER, response INTEGER, timeout INTEGER)")

        # self.mapto(self.init, "init")
        # self.mapto(self.online_offline, "scenario-churn")
        self.mapto(self.barter_record, "barter-record")
        # self.mapto(self.out_signature_request, "out-signature-request")
        # self.mapto(self.in_signature_response, "in-signature-response")
        # self.mapto(self.signature_timeout, "signature-timeout")
        self.mapto(self.walk_candidate, "walk-candidate")

    def start_parser(self, filename):
        super(BarterScenarioParser, self).start_parser(filename)

        # LAST_WALK contains the rowid of the last walk performed
        self.last_walk = 0

        # TIMESTEP is the walk number, i.e. this value is incremented by one for each walk performed
        self.timestep = 0

    #     self.my_member = ""
    #     self.my_class = ""
    #     self.load_stamp = 0.0
    #     self.load_cycle = 0
    #     self.edges = {}
    #     self.signatures = {}

    # def stop_parser(self, lineno):
    #     super(EffortScenarioParser, self).stop_parser(lineno)
    #     my_peer_id = self.peer_id
    #     def many():
    #         for (peer1, peer2), (global_time, origin, bits) in self.edges.iteritems():
    #             peer1_id = self.get_peer_id_from_mid(peer1)
    #             peer2_id = self.get_peer_id_from_mid(peer2)
    #             cycle = int(origin / self.cycle_size)
    #             while bits:
    #                 if bits & 1:
    #                     yield cycle, my_peer_id, peer1_id, peer2_id
    #                 bits >>= 1
    #                 cycle -= 1

    #     self.cur.executemany(u"INSERT INTO edge (cycle, peer, peer1, peer2) VALUES (?, ?, ?, ?)", list(many()))

    # def init(self, stamp, name, my_member, class_, cycle_size, malicious=[]):
    #     self.my_member = my_member
    #     self.my_class = class_
    #     self.cycle_size = cycle_size
    #     self.malicious = malicious

    #     self.load_stamp = stamp
    #     self.load_cycle = int(stamp / self.cycle_size)
    #     assert self.load_cycle > 0

    #     self.cur.execute(u"UPDATE peer SET class = ?, malicious = ? WHERE id = ?", (unicode(class_), ", ".join(map(str, malicious)), self.peer_id))

    # def online_offline(self, stamp, name, state, **kargs):
    #     if state == "offline":
    #         assert self.load_cycle > 0

    #         peer_id = self.peer_id
    #         begin = self.load_cycle
    #         end = int(stamp / self.cycle_size)
    #         self.cur.executemany(u"INSERT INTO online (cycle, peer) VALUES (?, ?)", ((cycle, peer_id) for cycle in xrange(begin, end + 1)))

    #         self.load_cycle = 0

    # def scenario_end(self, stamp, name):
    #     super(EffortScenarioParser, self).scenario_end(stamp, name)
    #     if self.load_cycle > 0:
    #         peer_id = self.peer_id
    #         begin = self.load_cycle
    #         end = int(stamp / self.cycle_size)
    #         self.cur.executemany(u"INSERT INTO online (cycle, peer) VALUES (?, ?)", ((cycle, peer_id) for cycle in xrange(begin, end + 1)))

    def barter_record(self, stamp, name, first_member, second_member, global_time, cycle, effort, upload_first_to_second, upload_second_to_first, first_timestamp, second_timestamp, **kargs):
        first_id = self.get_peer_id_from_public_key(first_member)
        second_id = self.get_peer_id_from_public_key(second_member)

        if first_id > second_id:
            # re-order everything
            first_id, second_id = second_id, first_id
            first_member, second_member = second_member, first_member
            upload_first_to_second, upload_second_to_first = upload_second_to_first, upload_first_to_second

        self.cur.execute(u"INSERT OR IGNORE INTO record (first, second, global_time, cycle, effort, upload_first_to_second, upload_second_to_first, avg_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                         (first_id, second_id, global_time, cycle, buffer(effort), upload_first_to_second, upload_second_to_first, (first_timestamp + second_timestamp) / 2.0))

        pair = (first_id, second_id)
        last_global_time, _ = self.last_record.get(pair, (0, 0))
        if global_time > last_global_time:
            self.last_record[pair] = (global_time, self.cur.lastrowid)

        id_, = self.cur.execute(u"SELECT id FROM record WHERE first = ? AND second = ? AND global_time = ?",
                                (first_id, second_id, global_time)).next()
        self.cur.execute(u"INSERT OR IGNORE INTO received_record (record, peer, timestamp, walk) VALUES (?, ?, ?, ?)",
                         (id_, self.peer_id, stamp, self.last_walk))

    def walk_candidate(self, stamp, name, lan_address, **kargs):
        self.timestep += 1
        # LAN_ADDRESS can be None when no destination peer was found
        if lan_address:
            self.cur.execute(u"INSERT INTO walk_candidate (peer, destination_peer, timestamp, timestep) VALUES (?, ?, ?, ?)",
                             (self.peer_id, self.get_peer_id_from_lan_address(lan_address, or_create=True), stamp, self.timestep))
        self.last_walk = self.cur.lastrowid

    def finish(self):
        if self.last_record:
            self.cur.executemany(u"INSERT INTO last_record (record) VALUES (?)",
                                 ((rowid,) for _, rowid in self.last_record.itervalues()))

    # def out_signature_request(self, stamp, name, identifier, member, destination):
    #     self.signatures[identifier] = stamp

    # def in_signature_response(self, stamp, name, identifier):
    #     self.cur.execute(u"INSERT INTO signature (peer, request, response) VALUES (?, ?, ?)", (self.peer_id, self.signatures.pop(identifier), stamp))

    # def signature_timeout(self, stamp, name, identifier):
    #     self.cur.execute(u"INSERT INTO signature (peer, request, timeout) VALUES (?, ?, ?)", (self.peer_id, self.signatures.pop(identifier), stamp))

def main():
    if len(sys.argv) == 4:
        database = sqlite3.Connection(sys.argv[3])

        # first pass
        parser = ScenarioParser1(database)
        parser.parse_directory(sys.argv[1], sys.argv[2], bzip2=True)

        # second pass
        parser = BarterScenarioParser(database)
        parser.parse_directory(sys.argv[1], sys.argv[2], bzip2=True)

        parser.finish()
        database.commit()

    else:
        print sys.argv[0], "IN-DIRECTORY IN-LOGFILE OUT-DATABASE"
        print sys.argv[0], "resultdir log try.db"

if __name__ == "__main__":
    main()
