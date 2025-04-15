//! Logic for controlling the rate at which data is sent

use crate::connection::RttEstimator;
use std::any::Any;
use std::sync::Arc;
use std::time::Instant;

mod bbr;
mod bbr2;
mod cubic;
mod new_reno;
mod pcc;

pub use bbr::{Bbr, BbrConfig};
pub use bbr2::{Bbr2, BbrConfig2};
pub use cubic::{Cubic, CubicConfig};
pub use new_reno::{NewReno, NewRenoConfig};
pub use pcc::{PccVivace, PccVivaceConfig};
/// Common interface for different congestion controllers
pub trait Controller: Send + Sync {
    /// One or more packets were just sent
    ///
    /// 属性宏，用于抑制编译器关于未使用变量的警告
    /// 参数：当前时间now，发送的字节数bytes，最后一个数据包的序号last_packet_number
    /// 有默认行为
    #[allow(unused_variables)]
    fn on_sent(&mut self, now: Instant, bytes: u64, last_packet_number: u64) {}
    /// Packet deliveries were confirmed
    ///
    /// `app_limited` indicates whether the connection was blocked on outgoing
    /// application data prior to receiving these acknowledgements.
    /// 参数：当前时间now，发送的时间sent，确认的字节数bytes，应用程序是否被限制app_limited，RTT估计器rtt
    /// 有默认行为
    #[allow(unused_variables)]
    fn on_ack(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        app_limited: bool,
        rtt: &RttEstimator,
    ) {
    }

    /// Packets are acked in batches, all with the same `now` argument. This indicates one of those batches has completed.
    /// 参数：当前时间now，在途数据inflight，是否应用程序被限制app_limited，最后一个确认的包的序号largest_packet_num_acked
    /// 有默认行为
    #[allow(unused_variables)]
    fn on_end_acks(
        &mut self,
        now: Instant,
        in_flight: u64,
        app_limited: bool,
        largest_packet_num_acked: Option<u64>,
    ) {
    }

    /// Packets were deemed lost or marked congested
    ///
    /// `in_persistent_congestion` indicates whether all packets sent within the persistent
    /// congestion threshold period ending when the most recent packet in this batch was sent were
    /// lost.
    /// `lost_bytes` indicates how many bytes were lost. This value will be 0 for ECN triggers.
    /// 参数：当前时间now，发送的时间sent，是否是持久性拥塞is_persistent_congestion，丢失的字节数lost_bytes
    fn on_congestion_event(
        &mut self,
        now: Instant,
        sent: Instant,
        is_persistent_congestion: bool,
        lost_bytes: u64,
    );

    /// The known MTU for the current network path has been updated
    fn on_mtu_update(&mut self, new_mtu: u16);

    /// Number of ack-eliciting bytes that may be in flight
    fn window(&self) -> u64;

    /// Duplicate the controller's state
    fn clone_box(&self) -> Box<dyn Controller>;

    /// Initial congestion window
    fn initial_window(&self) -> u64;

    /// Returns Self for use in down-casting to extract implementation details
    fn into_any(self: Box<Self>) -> Box<dyn Any>;

    /// return pacing window for connection/pacing
    fn pacing_window(&self) -> u64;
}

/// Constructs controllers on demand
pub trait ControllerFactory {
    /// Construct a fresh `Controller`
    fn build(self: Arc<Self>, now: Instant, current_mtu: u16) -> Box<dyn Controller>;
}

const BASE_DATAGRAM_SIZE: u64 = 1400; // 如果mtu没有更新，那么我们的算法会依据这个来计算。 此时，如果最小数据包大于BASE_DATAGRAM_SIZE，就会导致无法发送数据包，继而导致连接断开。
