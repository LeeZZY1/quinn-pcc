// use std::time::Duration;

// use crate::congestion::pcc::quic_time::QuicTime;
use std::time::Instant;


// 类型定义
pub(super) type HasRetransmittableData = bool;

pub(super) const HAS_RETRANSMITTABLE_DATA: HasRetransmittableData = true;

pub(super) type QuicByteCount = u64;
pub(super) type QuicPacketCount = u64;
pub(super) type QuicPacketLength = u64;
pub(super) type QuicPacketNumber = u64;


// AckedPacket 结构体
#[derive(Clone, Debug)]
pub struct AckedPacket {
    pub packet_number: QuicPacketNumber,
    pub bytes_acked: QuicPacketLength,
    pub receive_timestamp: Instant,
}

impl AckedPacket {
    pub(super) fn new(
        packet_number: QuicPacketNumber,
        bytes_acked: QuicPacketLength,
        receive_timestamp: Instant,
    ) -> Self {
        Self {
            packet_number,
            bytes_acked,
            receive_timestamp,
        }
    }
}

// LostPacket 结构体
#[derive(Clone, Debug)]
pub(super) struct LostPacket {
    pub packet_number: QuicPacketNumber,
    pub bytes_lost: QuicPacketLength,
}

impl LostPacket {
    pub(super) fn new(packet_number: QuicPacketNumber, bytes_lost: QuicPacketLength) -> Self {
        Self {
            packet_number,
            bytes_lost,
        }
    }
}

// 向量类型
pub(super) type AckedPacketVector = Vec<AckedPacket>;
pub(super) type LostPacketVector = Vec<LostPacket>;
