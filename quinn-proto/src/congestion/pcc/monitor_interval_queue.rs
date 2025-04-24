use std::collections::{LinkedList, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

// use crate::congestion::pcc::quic_time::Delta;  
// use crate::congestion::pcc::quic_time::QuicTime;
use crate::congestion::pcc::quic_type::{AckedPacket, QuicByteCount, QuicPacketNumber};  // 引入QuicByteCount类型,QuicByteCount在quic_types.rs中定义为u64
use crate::congestion::pcc::quic_bandwidth::QuicBandwidth;  // 引入QuicBandwidth类型,QuicBandwidth在quic_bandwidth.rs中定义

const K_MIN_RELIABLE_RTT: usize = 4;

#[derive(Debug, Clone)]
pub struct PacketRttSample {
    pub packet_number: QuicPacketNumber,
    pub sample_rtt: Duration,
    pub ack_timestamp: Instant,
    pub is_reliable: bool,
    pub is_reliable_for_gradient_calculation: bool,
}

impl Default for PacketRttSample {
    fn default() -> Self {
        Self {
            packet_number: 0,
            sample_rtt: Duration::ZERO,
            ack_timestamp: Instant::now(),
            is_reliable: false,
            is_reliable_for_gradient_calculation: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LostPacketSample {
    pub packet_number: QuicPacketNumber,
    pub bytes: QuicByteCount,
}

impl Default for LostPacketSample {
    fn default() -> Self {
        Self {
            packet_number: 0,
            bytes: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MonitorInterval {
    pub sending_rate: QuicBandwidth, // bits per second
    pub is_useful: bool,
    pub rtt_fluctuation_tolerance_ratio: f64,
    pub first_packet_sent_time: Instant,
    pub last_packet_sent_time: Instant,
    pub first_packet_number: QuicPacketNumber,
    pub last_packet_number: QuicPacketNumber,
    pub bytes_sent: u64,
    pub bytes_acked: u64,
    pub bytes_lost: u64,
    pub rtt_on_monitor_start: Duration,
    pub rtt_on_monitor_end: Duration,
    pub min_rtt: Duration,
    pub num_reliable_rtt: usize,
    pub num_reliable_rtt_for_gradient_calculation: usize,
    pub has_enough_reliable_rtt: bool,
    pub is_monitor_duration_extended: bool,
    pub packet_sent_intervals: Vec<Duration>,
    pub packet_rtt_samples: Vec<PacketRttSample>,
    pub lost_packet_samples: Vec<LostPacketSample>,
}

impl Default for MonitorInterval {
    fn default() -> Self {
        Self {
            sending_rate: QuicBandwidth::ZERO,
            is_useful: false,
            rtt_fluctuation_tolerance_ratio: 0.0,
            first_packet_sent_time: Instant::now(),
            last_packet_sent_time: Instant::now(),
            first_packet_number: 0,
            last_packet_number: 0,
            bytes_sent: 0,
            bytes_acked: 0,
            bytes_lost: 0,
            rtt_on_monitor_start: Duration::ZERO,
            rtt_on_monitor_end: Duration::ZERO,
            min_rtt: Duration::ZERO,
            num_reliable_rtt: 0,
            num_reliable_rtt_for_gradient_calculation: 0,
            has_enough_reliable_rtt: false,
            is_monitor_duration_extended: false,
            packet_sent_intervals: Vec::new(),
            packet_rtt_samples: Vec::new(),
            lost_packet_samples: Vec::new(),
        }
    }
}
 


pub(super) trait PccMonitorIntervalQueueDelegateInterface: Send + Sync {
    fn on_utility_available(&self, intervals: &[&MonitorInterval], event_time: Instant);
}

pub(super) struct MyDelegate;

impl PccMonitorIntervalQueueDelegateInterface for MyDelegate {
    fn on_utility_available(&self, intervals: &[&MonitorInterval], event_time: Instant) {
        let _ = event_time;
        // 实现你自己的逻辑
        println!("Utility is available with {} intervals", intervals.len());
    }
}

#[derive(Clone)]
pub(super) struct PccMonitorIntervalQueue {
    pending_rtt: Duration,
    pending_avg_rtt: Duration,
    pending_ack_interval: Duration,
    pending_event_time: Instant,
    burst_flag: bool,
    avg_interval_ratio: f64,
    num_useful_intervals: usize,
    num_available_intervals: usize,
    monitor_intervals: LinkedList<MonitorInterval>,
    pending_acked_packets: VecDeque<AckedPacket>,
    my_delegate: Arc<dyn PccMonitorIntervalQueueDelegateInterface>,
}

// impl Default for PccMonitorIntervalQueue {
//     fn default() -> Self {
//         Self {
//             pending_rtt: Duration::ZERO,
//             pending_avg_rtt: Duration::ZERO,
//             pending_ack_interval: Duration::ZERO,
//             pending_event_time: Instant::now(),
//             burst_flag: false,
//             avg_interval_ratio: -1.0,
//             num_useful_intervals: 0,
//             num_available_intervals: 0,
//             monitor_intervals: LinkedList::new(),
//             pending_acked_packets: VecDeque::new(),
//         }
//     }
// }

// #[derive(Debug, Clone)]
// pub(super) struct AckedPacket {
//     pub packet_number: u64,
//     pub bytes_acked: u64,
// }

// 传delegate参数
impl PccMonitorIntervalQueue {
    pub(super) fn new(delegate: Arc<dyn PccMonitorIntervalQueueDelegateInterface>) -> Self {
        Self {
            pending_rtt: Duration::ZERO,
            pending_avg_rtt: Duration::ZERO,
            pending_ack_interval: Duration::ZERO,
            pending_event_time: Instant::now(),
            burst_flag: false,
            avg_interval_ratio: -1.0,
            num_useful_intervals: 0,
            num_available_intervals: 0,
            monitor_intervals: LinkedList::new(),
            pending_acked_packets: VecDeque::new(),
            my_delegate: delegate,
        }
    }

    /// 当前interval的包序号范围
    /// 这个方法用于检查一个包是否在指定的interval范围内
    /// checked
    pub(super) fn interval_contains_packet(interval: &MonitorInterval, packet_number: u64) -> bool {
        packet_number >= interval.first_packet_number
            && packet_number <= interval.last_packet_number
    }

    pub(super) fn num_useful_intervals(&self) -> usize {
        self.num_useful_intervals
    }

    /// 这个方法用于检查一个interval是否有足够的可靠RTT
    /// checked
    pub(super) fn enqueue_new_monitor_interval(
        &mut self,
        sending_rate: QuicBandwidth,
        is_useful: bool,
        rtt_fluctuation_tolerance_ratio: f64,
        rtt: Duration,
    ) {
        eprintln!("enter enqueue_new_monitor_interval, useful: {:?}, useful_intervals : {:?}", is_useful, self.num_useful_intervals);
        if is_useful {
            self.num_useful_intervals += 1;
        }

        // 创建新的interval结构体
        let mut interval = MonitorInterval::default();
        interval.sending_rate = sending_rate;
        interval.is_useful = is_useful;
        interval.rtt_fluctuation_tolerance_ratio = rtt_fluctuation_tolerance_ratio;
        interval.rtt_on_monitor_start = rtt;
        interval.rtt_on_monitor_end = rtt;
        interval.min_rtt = rtt;

        self.monitor_intervals.push_back(interval);
    }

    /// 发包后调用
    /// checked
    pub(super) fn on_packet_sent(
        &mut self,
        sent_time: Instant,
        packet_number: u64,
        bytes: u64,
        sent_interval: Duration,
    ) {
        eprintln!("enter on_packet_sent, sent_time: {:?}, packet_number: {:?}, bytes: {:?}", sent_time, packet_number, bytes);
        // 在进入这里之前已经创建了interval，所以正常不会进入这个部分
        if self.monitor_intervals.is_empty() {
            eprintln!("OnPacketSent called with empty queue.");
            return;
        }

        // 拆包
        let interval = self.monitor_intervals.back_mut().unwrap();
        eprintln!(" | on_packet_sent | interval: {:?}", interval);
        if interval.bytes_sent == 0 {
            // 当前包是这个interval的第一个包
            eprintln!(" | on_packet_sent | first packet in interval");
            interval.first_packet_sent_time = sent_time;
            interval.first_packet_number = packet_number;
        }

        interval.last_packet_sent_time = sent_time;
        interval.last_packet_number = packet_number;
        interval.bytes_sent += bytes;
        interval.packet_sent_intervals.push(sent_interval);
    }

    /// 处理ack和丢包
    /// checked
    pub(super) fn on_congestion_event(
        &mut self,
        acked_packets: Vec<AckedPacket>,
        // lost_packets: Vec<LostPacketSample>,
        lost_bytes: u64,
        avg_rtt: Duration,
        latest_rtt: Duration,
        min_rtt: Duration,
        event_time: Instant,
        ack_interval: Duration,
    ) {
        // 将可获取间隔设置为0
        self.num_available_intervals = 0;

        // 如果没有有用的间隔，直接返回
        if self.num_useful_intervals == 0 {
            eprintln!(" | on_congestion_event | called with no useful intervals.");
            return;
        }

        let mut has_invalid_utility = false;
        for interval in &mut self.monitor_intervals {
            if !interval.is_useful {
                continue;
            }

            let utility_available = PccMonitorIntervalQueue::is_utility_available(interval);
            if utility_available {
                eprintln!(" | on_congestion_event | utility available");
                self.num_available_intervals += 1;
                continue;
            }
            // // Process lost packets
            // for lost_packet in &lost_packets {
            //     // 要知道丢包的包序号
            //     if Self::interval_contains_packet(interval, lost_packet.packet_number) {
            //         interval.bytes_lost += lost_packet.bytes;
            //         interval.lost_packet_samples.push(lost_packet.clone());
            //     }
            // }
            interval.bytes_lost += lost_bytes;       

            for acked_packet in &self.pending_acked_packets {
                if Self::interval_contains_packet(interval, acked_packet.packet_number) {
                    if interval.bytes_acked == 0 {
                        // This is the RTT before starting sending at interval.sending_rate.
                        interval.rtt_on_monitor_start = self.pending_avg_rtt;
                    }
                    interval.bytes_acked += acked_packet.bytes_acked;
            
                    let mut is_reliable = false;
                    // 不为0才能进入
                    if !self.pending_ack_interval.is_zero() {
                        let mut interval_ratio = self.pending_ack_interval.as_micros() as f64
                            / ack_interval.as_micros() as f64;
            
                        if interval_ratio < 1.0 {
                            interval_ratio = 1.0 / interval_ratio;
                        }
                        
                        // 初始化avg_interval_ratio为-1
                        if self.avg_interval_ratio < 0.0 {
                            self.avg_interval_ratio = interval_ratio;
                        }
            
                        if interval_ratio > 50.0 * self.avg_interval_ratio {
                            self.burst_flag = true;
                        } else if self.burst_flag {
                            if latest_rtt > self.pending_rtt
                                && self.pending_rtt < self.pending_avg_rtt
                            {
                                self.burst_flag = false;
                            }
                        } else {
                            is_reliable = true;
                            interval.num_reliable_rtt += 1;
                        }
            
                        self.avg_interval_ratio =
                            self.avg_interval_ratio * 0.9 + interval_ratio * 0.1;
                    }
            
                    let mut is_reliable_for_gradient_calculation = false;
                    if is_reliable {
                        // if self.latest_rtt > self.pending_rtt {
                        is_reliable_for_gradient_calculation = true;
                        interval.num_reliable_rtt_for_gradient_calculation += 1;
                    }
            
                    interval.packet_rtt_samples.push(PacketRttSample {
                        packet_number: acked_packet.packet_number,
                        sample_rtt: self.pending_rtt,
                        ack_timestamp: self.pending_event_time,
                        is_reliable,
                        is_reliable_for_gradient_calculation,
                    });

                    if interval.num_reliable_rtt >= K_MIN_RELIABLE_RTT {
                        interval.has_enough_reliable_rtt = true;
                    }
                }
            }
            

            if PccMonitorIntervalQueue::is_utility_available(interval) {
                interval.rtt_on_monitor_end = avg_rtt;
                interval.min_rtt = min_rtt;
                has_invalid_utility = Self::has_invalid_utility(interval);
                if self.num_available_intervals >= self.num_useful_intervals {
                    break;
                }
                self.num_available_intervals += 1;
                assert!(self.num_available_intervals <= self.num_useful_intervals);
            }
        }

        self.pending_acked_packets.clear();
        self.pending_acked_packets.extend(acked_packets.iter().cloned());

        self.pending_rtt = latest_rtt;
        self.pending_avg_rtt = avg_rtt;
        self.pending_ack_interval = ack_interval;
        self.pending_event_time = event_time;

        if self.num_useful_intervals > self.num_available_intervals && !has_invalid_utility {
            return;
        }

        if !has_invalid_utility {

            assert!(self.num_available_intervals > 0);

            let useful_intervals: Vec<&MonitorInterval> = self
                .monitor_intervals
                .iter()
                .filter(|i| i.is_useful)
                .collect();

            assert!(self.num_available_intervals == useful_intervals.len());

            eprintln!("call on_utility_available impl in pcc mod.rs");
            self.my_delegate
                .on_utility_available(&useful_intervals, event_time);
        }

        // Remove processed intervals
        while let Some(interval) = self.monitor_intervals.front() {
            if interval.is_useful {
                eprintln!(" | on_congestion_event | remove used interval");
                self.num_useful_intervals -= 1;
            }
            self.monitor_intervals.pop_front();
        }
        self.num_available_intervals = 0;
    }

    /// 检查interval是否有足够的可靠RTT且所有发送的包已被处理
    /// checked
    fn is_utility_available(interval: &MonitorInterval) -> bool {
        interval.has_enough_reliable_rtt
            && (interval.bytes_acked + interval.bytes_lost) == interval.bytes_sent
    }

    /// 检查interval是否有无效的utility
    /// checked
    fn has_invalid_utility(interval: &MonitorInterval) -> bool {
        interval.first_packet_sent_time == interval.last_packet_sent_time
    }
}

// 其他辅助方法和接口实现
/// 监控间隔队列
/// checked
impl PccMonitorIntervalQueue {
    /// 获取第一个interval
    /// checked
    pub(super) fn front(&self) -> Option<&MonitorInterval> {
        assert!(!self.monitor_intervals.is_empty());
        self.monitor_intervals.front()
    }

    /// 获取最后一个interval
    /// checked
    pub(super) fn current(&self) -> Option<&MonitorInterval> {
        assert!(!self.monitor_intervals.is_empty());
        // 从 monitor_intervals 这个容器中返回当前（最后一个）MonitorInterval，
        // 它返回的是一个 Option<&MonitorInterval>
        self.monitor_intervals.back()
    }

    /// 延长当前interval的持续时间
    /// checked
    pub(super) fn extend_current_interval(&mut self) {
        assert!(!self.monitor_intervals.is_empty(), "monitor_intervals is empty");
        if let Some(interval) = self.monitor_intervals.back_mut() {
            interval.is_monitor_duration_extended = true;
        }
    }

    /// 判断当前interval队列是否为空
    /// checked
    pub(super) fn is_empty(&self) -> bool {
        self.monitor_intervals.is_empty()
    }

    /// 获取当前interval的数量
    /// checked
    pub(super) fn size(&self) -> usize {
        self.monitor_intervals.len()
    }

    /// 在启动阶段出现RTT膨胀时调用
    /// checked
    pub(super) fn on_rtt_inflation_in_starting(&mut self) {
        // 清空监控队列
        eprintln!("on_rtt_inflation_in_starting");
        eprintln!(" | on_rtt_inflation_in_starting | clear monitor_intervals : {:?}", self.monitor_intervals.len());
        self.monitor_intervals.clear();
        self.num_useful_intervals = 0;
        self.num_available_intervals = 0;
    }
}
