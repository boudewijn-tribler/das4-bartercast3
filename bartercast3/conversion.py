from struct import pack, unpack_from

from .efforthistory import EffortHistory, CYCLE_SIZE

from dispersy.member import Member
from dispersy.conversion import BinaryConversion
from dispersy.message import DropPacket
from dispersy.logger import get_logger
logger = get_logger(__name__)

class BarterConversion(BinaryConversion):
    def __init__(self, community):
        super(BarterConversion, self).__init__(community, "\x01")
        self.define_meta_message(chr(1), community.get_meta_message(u"barter-record"), self._encode_barter_record, self._decode_barter_record)
        self.define_meta_message(chr(2), community.get_meta_message(u"ping"), self._encode_ping_pong, self._decode_ping_pong)
        self.define_meta_message(chr(3), community.get_meta_message(u"pong"), self._encode_ping_pong, self._decode_ping_pong)
        self.define_meta_message(chr(4), community.get_meta_message(u"upload"), self._encode_upload, self._decode_upload)

    def _encode_barter_record(self, message):
        payload = message.payload
        bytes_ = payload.effort.bytes

        return (pack(">LQQB",
                     long(payload.cycle),
                     long(payload.upload_first_to_second),
                     long(payload.upload_second_to_first),
                     len(bytes_)),
                bytes_,
                # the following parameters are used for debugging only
                pack(">LLQQQQ",
                     long(payload.first_timestamp),
                     long(payload.second_timestamp),
                     long(payload.first_upload),
                     long(payload.first_download),
                     long(payload.second_upload),
                     long(payload.second_download)))

    def _decode_barter_record(self, placeholder, offset, data):
        if len(data) < offset + 21:
            raise DropPacket("Insufficient packet size (_decode_barter_record)")

        cycle, upload_first_to_second, upload_second_to_first, length = unpack_from(">LQQB", data, offset)
        offset += 21

        if len(data) < offset + length:
            raise DropPacket("Insufficient packet size (_decode_barter_record)")
        effort = EffortHistory(data[offset:offset+length], cycle * CYCLE_SIZE)
        offset += length

        # the following parameters are used for debugging only
        if len(data) < offset + 40:
            raise DropPacket("Insufficient packet size (_decode_barter_record)")
        first_timestamp, second_timestamp, first_upload, first_download, second_upload, second_download = unpack_from(">LLQQQQ", data, offset)
        offset += 40

        first_timestamp = float(first_timestamp)
        second_timestamp = float(second_timestamp)

        return offset, placeholder.meta.payload.implement(cycle, effort, upload_first_to_second, upload_second_to_first,
                                                          # the following parameters are used for debugging only
                                                          float(first_timestamp), float(second_timestamp), first_upload, first_download, second_upload, second_download)

    def _encode_ping_pong(self, message):
        payload = message.payload
        return self._struct_BH.pack(len(payload.member.public_key), payload.identifier), payload.member.public_key

    def _decode_ping_pong(self, placeholder, offset, data):
        if len(data) < offset + 3:
            raise DropPacket("Insufficient packet size (_decode_ping_pong)")

        key_length, identifier, = self._struct_BH.unpack_from(data, offset)
        offset += 3

        if len(data) < offset + key_length:
            raise DropPacket("Insufficient packet size (_decode_ping_pong)")
        try:
            member = Member(data[offset:offset+key_length])
        except:
            raise DropPacket("Invalid public key (_decode_ping_pong)")
        offset += key_length

        return offset, placeholder.meta.payload.Implementation(placeholder.meta.payload, identifier, member)

    def _encode_upload(self, message):
        payload = message.payload
        return pack(">LB", payload.amount, len(payload.member.public_key)), payload.member.public_key

    def _decode_upload(self, placeholder, offset, data):
        if len(data) < offset + 5:
            raise DropPacket("Insufficient packet size (_decode_upload)")

        amount, key_length, = unpack_from(">LB", data, offset)
        offset += 5

        if len(data) < offset + key_length:
            raise DropPacket("Insufficient packet size (_decode_upload)")
        try:
            member = self._community.dispersy.get_member(data[offset:offset+key_length])
        except Exception as exception:
            raise DropPacket("Invalid public key (_decode_upload.  %s)" % (str(exception),))
        offset += key_length

        return offset, placeholder.meta.payload.Implementation(placeholder.meta.payload, amount, member)
