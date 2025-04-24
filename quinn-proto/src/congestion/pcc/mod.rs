use super::{Controller, ControllerFactory, BASE_DATAGRAM_SIZE};
use crate::connection::RttEstimator;
use crate::Duration;
use std::str::ParseBoolError;
use std::{any::Any, ops::Sub};
use std::sync::Arc;
use std::time::Instant;
// Define or import the missing types

mod monitor_interval_queue;
use monitor_interval_queue::{ 
    MonitorInterval, MyDelegate, PccMonitorIntervalQueue,
    PccMonitorIntervalQueueDelegateInterface,
};
mod utility_manager;
use utility_manager::PccUtilityManager;

mod quic_bandwidth;
use quic_bandwidth::QuicBandwidth;

// mod quic_time;
// use quic_time::{QuicTime, Delta};

mod quic_type;
use quic_type::{AckedPacket, LostPacket,AckedPacketVector, LostPacketVector};


// 使用外部定义的 Delta 结构体创建常量
const K_INITIAL_RTT: Duration = Duration::from_millis(100); // 100ms
const K_MEGABIT: u64 = 1024 * 1024;
const K_DECISION_MADE_STEP_SIZE: f64 = 0.02;
const K_PROBING_STEP_SIZE: f64 = 0.05;
const K_MAX_DECISION_MADE_STEP_SIZE: f64 = 0.1;
const K_UTILITY_GRADIENT_TO_RATE_CHANGE_FACTOR: f64 = 1.0;
const K_RATE_CHANGE_AMPLIFY_EXPONENT: f64 = 1.2;
const FLAGS_RESTORE_CENTRAL_RATE_UPON_APP_LIMITED: bool = false;
const K_INITIAL_MAX_STEP_SIZE: f64 = 0.05;
const K_INCREMENTAL_STEP_SIZE: f64 = 0.05;
const K_MIN_RATE_CHANGE: u64 = 500 * 1024; // bits per second
const K_MIN_SENDING_RATE: u64 = 500 * 1024; // bits per second
const K_MIN_RELIABILITY_RATIO: f64 = 0.8;

const FLAGS_ENABLE_RTT_DEVIATION_BASED_EARLY_TERMINATION: bool = true;
const FLAGS_TRIGGER_EARLY_TERMINATION_BASED_ON_INTERVAL_QUEUE_FRONT: bool = false;
const FLAGS_ENABLE_EARLY_TERMINATION_BASED_ON_LATEST_RTT_TREND: bool = false;
const FLAGS_MAX_RTT_FLUCTUATION_TOLERANCE_RATIO_IN_STARTING: f64 = 100.0;
const FLAGS_MAX_RTT_FLUCTUATION_TOLERANCE_RATIO_IN_DECISION_MADE: f64 = 1.0;
const FLAGS_RTT_FLUCTUATION_TOLERANCE_GAIN_IN_STARTING: f64 = 2.5;
const FLAGS_RTT_FLUCTUATION_TOLERANCE_GAIN_IN_DECISION_MADE: f64 = 1.5;
const FLAGS_RTT_FLUCTUATION_TOLERANCE_GAIN_IN_PROBING: f64 = 1.0;
// const  FLAGS_CAN_SEND_RESPECT_CONGESTION_WINDOW: bool =  true;
// const  FLAGS_BYTES_IN_FLIGHT_GAIN: f64 =  2.5;
// const  FLAGS_EXIT_STARTING_BASED_ON_SAMPLED_BANDWIDTH: bool =  false;


// PccVivace模式
#[derive(Debug, Clone, Copy, PartialEq)]
enum Mode {
    Starting,
    Probing,
    DecisionMade,
}

/// PccVivace 速度变化方向
#[derive(Debug, Clone, Copy, PartialEq)]
enum Direction {
    Increase,
    Decrease,
}

/// PccVivace 效用信息
#[derive(Clone)]
pub struct UtilityInfo {
    sending_rate: QuicBandwidth, // bits per second
    utility: f64,
}

// 默认初始化为0
impl Default for UtilityInfo {
    fn default() -> Self {
        Self {
            sending_rate: QuicBandwidth::ZERO,
            utility: 0.0,
        }
    }
}

/// PccVivace 变量
#[derive(Clone)]
#[allow(dead_code)]
pub struct PccVivace {
    config: Arc<PccVivaceConfig>,
    conn_start_time: Option<Instant>,
    monitor_duration: Duration,
    rtt_deviation: Duration,
    min_rtt_deviation: Duration,
    latest_rtt: Duration,
    min_rtt: Duration,
    avg_rtt: Duration,
    max_cwnd_bytes: u64,
    delegate: Arc<dyn PccMonitorIntervalQueueDelegateInterface>,
    monitor_queue: PccMonitorIntervalQueue,
    utility_manager: PccUtilityManager,
    minimum_rate: u64,
    sending_rate: QuicBandwidth,
    mode: Mode,
    direction: Direction,
    rounds: u32,
    incremental_rate_change_step_allowance: u32,
    latest_utility_info: UtilityInfo,
    current_mtu: u64,
    congestion_window: u64, // 添加此字段
    initial_congestion_window: u64,
    latest_ack_timestamp: Instant,
    latest_sent_timestamp: Instant,
    rtt_updated: bool,
    has_seen_valid_rtt: bool,
    rtt_on_inflation_start: Option<Duration>,
    ack_packets: AckedPacketVector,
    lost_packets: LostPacketVector,
    largest_packet_num_acked: Option<u64>,
}

impl PccVivace {
    /// 创建新的 PccVivace 实例
    pub fn new(config: Arc<PccVivaceConfig>, now: Instant, current_mtu: u16) -> Self {
        let initial_window: u64 = config.initial_congestion_window;
        let delegate: Arc<dyn PccMonitorIntervalQueueDelegateInterface> = Arc::new(MyDelegate);
        let monitor_queue = PccMonitorIntervalQueue::new(delegate.clone());

        Self {
            config,
            current_mtu: current_mtu as u64,
            conn_start_time: None,
            monitor_duration: Duration::from_micros(0),
            rtt_deviation: Duration::from_micros(0),
            min_rtt_deviation: Duration::from_micros(0),
            latest_rtt: Duration::from_micros(0),
            min_rtt: Default::default(),
            avg_rtt: Duration::from_micros(0),
            max_cwnd_bytes: 0,
            delegate,
            monitor_queue,
            utility_manager: PccUtilityManager::new(),
            minimum_rate: 1024,
            // 单位是bps
            sending_rate: QuicBandwidth::from_bytes_and_time_delta(
                initial_window * BASE_DATAGRAM_SIZE , &K_INITIAL_RTT
            ),
            //sending_rate: initial_window as f64 * 8.0 / K_INITIAL_RTT,
            latest_utility_info: Default::default(),
            mode: Mode::Starting,
            direction: Direction::Increase,
            rounds: 1,
            incremental_rate_change_step_allowance: 0, // Provide a default value
            congestion_window: initial_window,
            initial_congestion_window: initial_window, // Use initial_window here
            latest_ack_timestamp: now,
            latest_sent_timestamp: now,
            rtt_updated: false,
            has_seen_valid_rtt: false,
            rtt_on_inflation_start: None,
            ack_packets: AckedPacketVector::new(),
            lost_packets: LostPacketVector::new(),
            largest_packet_num_acked: None,
        }
    }

    /// 创建新的监控间隔
    /// checked
    pub fn create_new_interval(&mut self, event_time: Instant) -> bool {
        eprintln!("enter create_new_interval");
        // 如果监控间隔队列为空，则创建新的间隔
        // 初次启动时会被调用
        if self.monitor_queue.is_empty() {
            eprintln!("monitor_queue_is_empty");
            return true;
        }

        // 如果没有 RTT 数据可用，返回 false
        if self.latest_rtt.is_zero() {
            eprintln!("latest_rtt == zero");
            return false;
        }

        // 如果队列中没有有用的间隔，创建新的有用间隔
        if self.monitor_queue.num_useful_intervals() == 0 {
            eprintln!("self.monitor_queue.num_useful_intervals() == 0");
            return true;
        }

        // 获取当前的间隔
        let current_interval = &self.monitor_queue.current(); // 假设我们正在检查队列中的第一个间隔

        // 如果当前间隔是无用的，不创建新的间隔
        if let Some(interval) = current_interval {
            if !interval.is_useful {
                eprintln!("interval.is_useful == 0");
                return false;
            }
        } 

        // 如果当前有用间隔没有足够的 RTT 数据或者持续时间没有超过监控时间，不创建新的间隔
        // event_time - interval.first_packet_sent_time
        if let Some(interval) = current_interval {
            if !interval.has_enough_reliable_rtt
                || event_time.sub(interval.first_packet_sent_time)  < self.monitor_duration
            {
                return false;
            }
        } 
        // 如果当前间隔的 RTT 数据不可靠，或者持续时间没有超过监控时间，不创建新的间隔
        let current_interval = self.monitor_queue.current();
        let reliability_ratio = current_interval.unwrap().num_reliable_rtt as f64
                / current_interval.unwrap().packet_rtt_samples.len() as f64;
        if reliability_ratio > K_MIN_RELIABILITY_RATIO {
            return true;
        } else if current_interval.unwrap().is_monitor_duration_extended {
            return true;
        } else {
            self.monitor_duration = self.monitor_duration * 2;
            self.monitor_queue.extend_current_interval();
            return false;
        }
         
    }

    fn maybe_set_sending_rate(&mut self) {
        // 只在 PROBING 模式下可能调整发送速率
        if self.mode != Mode::Probing
            || (self.monitor_queue.num_useful_intervals()
                == 2 * self.get_num_interval_groups_in_probing()
                && !self.monitor_queue.current().map_or(false, |i| i.is_useful))
        {
            return;
        }

        if self.monitor_queue.num_useful_intervals() != 0 {
            // 恢复中心发送速率
            self.restore_central_sending_rate();

            if self.monitor_queue.num_useful_intervals()
                == 2 * self.get_num_interval_groups_in_probing()
            {
                // 当前是第一个无用间隔，其发送速率是中心速率
                return;
            }
        }

        // 构建多个监测组，每组包含一个 INCREASE 和一个 DECREASE
        if self.monitor_queue.num_useful_intervals() % 2 == 0 {
            self.direction = if rand::random::<bool>() {
                Direction::Increase
            } else {
                Direction::Decrease
            };
        } else {
            self.direction = match self.direction {
                Direction::Increase => Direction::Decrease,
                Direction::Decrease => Direction::Increase,
            };
        }

        if self.direction == Direction::Increase {
            self.sending_rate = self.sending_rate * (1.0 + K_PROBING_STEP_SIZE);
        } else {
            self.sending_rate = self.sending_rate * (1.0 - K_PROBING_STEP_SIZE);
        }
    }

    // /// return backup rate when no useful interval
    // pub fn get_sending_rate_for_non_useful_interval(&self) -> f64 {
    //     eprintln!("enter get_sending_rate_for_non_useful_interval")
    //     self.sending_rate
    // }
    /// 返回无用间隔的发送速率
    /// 若为启动阶段，速率减半
    /// 若为探测阶段，速率减小
    /// 若为决策阶段，速率根据变化方向改变
    /// checked
    pub fn get_sending_rate_for_non_useful_interval(&self) -> QuicBandwidth {
        eprintln!("enter get_sending_rate_for_non_useful_interval");
        match self.mode {
            Mode::Starting => {
                // Use halved sending rate
                self.sending_rate * 0.5
            }
            Mode::Probing => {
                // Use smaller probing rate
                self.sending_rate * (1.0 - K_PROBING_STEP_SIZE)
            }
            Mode::DecisionMade => {
                if self.direction == Direction::Decrease {
                    self.sending_rate
                } else {
                    let step = (self.rounds as f64 * K_DECISION_MADE_STEP_SIZE)
                        .min(K_MAX_DECISION_MADE_STEP_SIZE);
                    self.sending_rate * (1.0 / (1.0 + step))
                }
            }
        }
    }

    /// restore
    pub fn restore_central_sending_rate(&mut self) {
        match self.mode {
            Mode::Starting => {
                eprintln!("Attempt to set probing rate while in STARTING");
            }
            Mode::Probing => {
                if let Some(current) = self.monitor_queue.current() {
                    if current.is_useful {
                        // 执行恢复 central 发送速率的逻辑
                        if self.direction == Direction::Increase {
                            self.sending_rate = self.sending_rate
                                * (1.0 / (1.0 + K_PROBING_STEP_SIZE));
                        } else {
                            self.sending_rate = self.sending_rate 
                                * (1.0 / (1.0 - K_PROBING_STEP_SIZE));
                        }
                    }
                }
            }
            Mode::DecisionMade => {
                let step = f64::min(
                    self.rounds as f64 * K_DECISION_MADE_STEP_SIZE,
                    K_MAX_DECISION_MADE_STEP_SIZE,
                );
                if self.direction == Direction::Increase {
                    self.sending_rate =
                        self.sending_rate  * (1.0 / (1.0 + step));
                } else {
                    self.sending_rate =
                        self.sending_rate * (1.0 / (1.0 - step));
                }
            }
        }
    }

    /// 进入探测模式
    /// 1. 如果当前模式是 Starting，则将发送速率减半
    /// 2. 如果当前模式是 DecisionMade 或 Probing，则还原中心发送速率
    /// 3. 如果当前模式是 Probing，则增加 rounds
    /// 4. 如果当前模式是 DecisionMade，则将 rounds 设置为 1
    /// 5. 如果效用标签是 Hybrid，则设置有效的效用标签
    fn enter_probing(&mut self) {
        //     self.mode = Mode::Probing;
        //     self.rounds = 1;
        //     self.incremental_rate_change_step_allowance = 0;
        //     self.sending_rate = cmp::max(self.sending_rate, K_MIN_SENDING_RATE);
        eprintln!("enter enter_probing");
        match self.mode {
            Mode::Starting => {
                eprintln!(" | enter_probing | Starting mode, reducing sending rate by half");
                // 当前发送速率减半
                self.sending_rate = self.sending_rate * 0.5 ;
            }
            Mode::DecisionMade | Mode::Probing => {
                eprintln!(" | enter_probing | DecisionMade or Probing mode, restoring central sending rate");
                // 还原中心发送速率
                self.restore_central_sending_rate();
            }
        }

        if self.mode == Mode::Probing {
            eprintln!(" | enter_probing | already in Probing mode, incrementing rounds");
            self.rounds += 1;
            return;
        }

        eprint!(" | enter_probing | setting Probing mode");
        self.mode = Mode::Probing;
        self.rounds = 1;
    }

    /// 获取当前间隔数量
    /// checked
    pub fn get_num_interval_groups_in_probing(&self) -> usize {
        return 3;
    }

    /// 判断是否可以做决策
    /// 参数：效用信息utility_info
    /// 返回值：是否可以做决策
    pub fn can_make_decision(&self, utility_info: &[UtilityInfo]) -> bool {
        // 判断是否有足够的 interval group 来进行决策
        // 这里的 interval group 数量不应小于 2*3
        if utility_info.len() < 2 * self.get_num_interval_groups_in_probing() {
            return false;
        }

        let mut increase = false;

        for i in 0..self.get_num_interval_groups_in_probing() {
            let a = &utility_info[2 * i];
            let b = &utility_info[2 * i + 1];

            let increase_i = if a.utility > b.utility {
                a.sending_rate > b.sending_rate
            } else {
                a.sending_rate < b.sending_rate
            };

            if i == 0 {
                increase = increase_i;
            }

            // 如果有不一致的判断，不能做决策
            if increase_i != increase {
                return false;
            }
        }

        true
    }

    /// 设置速率变化方向
    /// 参数：效用信息utility_info
    /// 1. 遍历效用信息，统计增加和减少的次数
    /// 2. 根据增加和减少的次数，设置速率变化方向
    /// 3. 遍历效用信息，更新最新的效用信息
    pub fn set_rate_change_direction(&mut self, utility_info: &[UtilityInfo]) {
        let mut count_increase = 0;
        let mut count_decrease = 0;
        let num_groups = self.get_num_interval_groups_in_probing();

        for i in 0..num_groups {
            let u0 = &utility_info[2 * i];
            let u1 = &utility_info[2 * i + 1];

            // 判断效用信息的发送速率和效用值的变化方向
            let increase_i = if u0.utility > u1.utility {
                u0.sending_rate > u1.sending_rate
            } else {
                u0.sending_rate < u1.sending_rate
            };

            if increase_i {
                count_increase += 1;
            } else {
                count_decrease += 1;
            }
        }

        self.direction = if count_increase > count_decrease {
            Direction::Increase
        } else {
            Direction::Decrease
        };

        // 更新 latest_utility_info_
        for i in 0..num_groups {
            let u0 = &utility_info[2 * i];
            let u1 = &utility_info[2 * i + 1];
 
            let increase_i = if u0.utility > u1.utility {
                u0.sending_rate > u1.sending_rate
            } else {
                u0.sending_rate < u1.sending_rate
            };

            if (increase_i && self.direction == Direction::Increase)
                || (!increase_i && self.direction == Direction::Decrease)
            {
                self.latest_utility_info = if u0.utility > u1.utility {
                    u0.clone()
                } else {
                    u1.clone()
                };
            }
        }
    }

    /// 创建有用的间隔
    /// checked
    fn create_useful_interval(&self) -> bool {
        eprintln!("enter create_useful_interval: ");
        if self.avg_rtt.as_micros() == 0 {
            // 没有 RTT 数据，说明刚开始连接，不能创建 useful interval
            assert!(self.mode == Mode::Starting);
            return false;
        }

        // STARTING 和 DECISION_MADE 模式下最多允许1个 useful interval
        // PROBING 模式下最多允许 2 * 3组数个 useful interval
        let max_useful = if self.mode == Mode::Probing {
            2 * self.get_num_interval_groups_in_probing()
        } else {
            1
        };

        // 如果有用间隔小于最大有用间隔数量，就可以创建有用间隔
        self.monitor_queue.num_useful_intervals() < max_useful
    }

    /// 获取最大 RTT 波动容忍度
    /// checked
    fn get_max_rtt_fluctuation_tolerance(&self) -> f64 {
        eprintln!("get_max_rtt_fluctuation_tolerance");
        // 1. 基础容忍比率
        let mut tolerance_ratio = match self.mode {
            Mode::Starting => FLAGS_MAX_RTT_FLUCTUATION_TOLERANCE_RATIO_IN_STARTING,
            _ => FLAGS_MAX_RTT_FLUCTUATION_TOLERANCE_RATIO_IN_DECISION_MADE,
        };

        // 2. 如果启用了 RTT 偏差控制，默认为true
        if FLAGS_ENABLE_RTT_DEVIATION_BASED_EARLY_TERMINATION {
            let tolerance_gain = match self.mode {
                // 2.5
                Mode::Starting => FLAGS_RTT_FLUCTUATION_TOLERANCE_GAIN_IN_STARTING,
                // 1
                Mode::Probing => FLAGS_RTT_FLUCTUATION_TOLERANCE_GAIN_IN_PROBING,
                // 1.5
                Mode::DecisionMade => FLAGS_RTT_FLUCTUATION_TOLERANCE_GAIN_IN_DECISION_MADE,
            };

            let rtt_dev_us = self.rtt_deviation.as_micros();
            let avg_rtt_us = if self.avg_rtt.is_zero() {
                K_INITIAL_RTT.as_micros()
            } else {
                self.avg_rtt.as_micros()
            };

            let dynamic_ratio = tolerance_gain * rtt_dev_us as f64 / avg_rtt_us as f64;
            tolerance_ratio = tolerance_ratio.min(dynamic_ratio);
        }

        tolerance_ratio
    }

    /// 计算速率变化
    /// 参数：效用信息utility_info
    /// 返回值：速率变化
    /// 1. 如果当前模式是 Starting，则返回 0
    /// 2. 如果当前模式是 Probing，则计算速率变化
    /// 3. 如果当前模式是 DecisionMade，则计算速率变化
    /// 4. 如果速率变化大于最大允许速率变化，则将速率变化设置为最大允许速率变化
    pub fn compute_rate_change(&mut self, utility_info: &[UtilityInfo]) -> QuicBandwidth {
        eprintln!("enter compute_rate_change");
        assert!(self.mode != Mode::Starting);

        let delta_sending_rate;
        let mut delta_utility = 0.0;

        if self.mode == Mode::Probing {
            delta_sending_rate =
                QuicBandwidth::max(utility_info[0].sending_rate, utility_info[1].sending_rate)
                    - QuicBandwidth::min(utility_info[0].sending_rate, utility_info[1].sending_rate);

            for i in 0..self.get_num_interval_groups_in_probing() {
                let increase_i = if utility_info[2 * i].utility > utility_info[2 * i + 1].utility {
                    utility_info[2 * i].sending_rate > utility_info[2 * i + 1].sending_rate
                } else {
                    utility_info[2 * i].sending_rate < utility_info[2 * i + 1].sending_rate
                };

                if (increase_i && self.direction == Direction::Decrease)
                    || (!increase_i && self.direction == Direction::Increase)
                {
                    continue;
                }

                delta_utility +=
                    f64::max(utility_info[2 * i].utility, utility_info[2 * i + 1].utility)
                        - f64::min(utility_info[2 * i].utility, utility_info[2 * i + 1].utility);
            }
            delta_utility /= self.get_num_interval_groups_in_probing() as f64;
        } else {
            delta_sending_rate = QuicBandwidth::max(
                utility_info[0].sending_rate,
                self.latest_utility_info.sending_rate,
            ) - QuicBandwidth::min(
                utility_info[0].sending_rate,
                self.latest_utility_info.sending_rate,
            );
            delta_utility = f64::max(utility_info[0].utility, self.latest_utility_info.utility)
                - f64::min(utility_info[0].utility, self.latest_utility_info.utility);
        }

        assert!(delta_sending_rate != QuicBandwidth::ZERO);

        let utility_gradient = (K_MEGABIT as f64) * delta_utility / (delta_sending_rate.to_bits_per_second() as f64);
        let mut rate_change = QuicBandwidth::from_bits_per_second(
            (utility_gradient * K_MEGABIT as f64 * K_UTILITY_GRADIENT_TO_RATE_CHANGE_FACTOR) as u64,
        );

        if self.mode == Mode::DecisionMade {
            rate_change = rate_change
                * (f64::powi(
                    (self.rounds + 1) as f64 / 2.0,
                    K_RATE_CHANGE_AMPLIFY_EXPONENT as i32,
                ) as f64);
        } else {
            self.incremental_rate_change_step_allowance = 0;
        }

        let max_allowed_rate_change = self.sending_rate
            * (K_INITIAL_MAX_STEP_SIZE
                + K_INCREMENTAL_STEP_SIZE * self.incremental_rate_change_step_allowance as f64);

        if rate_change > max_allowed_rate_change {
            rate_change = max_allowed_rate_change;
            self.incremental_rate_change_step_allowance += 1;
        } else if self.incremental_rate_change_step_allowance > 0 {
            self.incremental_rate_change_step_allowance -= 1;
        }
        
        QuicBandwidth::max(rate_change, QuicBandwidth::from_bits_per_second(K_MIN_RATE_CHANGE))
    }

    /// 进入决策已做出状态
    /// 参数：效用信息utility_info
    /// 1. 如果当前模式是 Probing，则根据速率变化方向调整发送速率
    /// 2. 如果当前模式是 Probing，则将 rounds 设置为 1
    /// 3. 计算速率变化
    /// 4. 根据速率变化方向调整发送速率
    /// 5. 如果发送速率小于最小发送速率，则将发送速率设置为最小发送速率
    ///                             将模式设置为 Probing
    ///                             将 rounds 设置为 1
    ///                             将增量速率变化步骤允许值设置为 0
    /// 6. 否则，将模式设置为 DecisionMade
    pub fn enter_decision_made(&mut self, utility_info: &[UtilityInfo]) {
        eprintln!("enter enter_decision_made");
        if self.mode == Mode::Probing {
            self.sending_rate = if self.direction == Direction::Increase {
                eprintln!(" | enter_decision_made | Increase rate");
                self.sending_rate * (1.0 + K_PROBING_STEP_SIZE)
            } else {
                eprintln!(" | enter_decision_made | Decrease rate");
                self.sending_rate * (1.0 - K_PROBING_STEP_SIZE)
            };
        }

        self.rounds = if self.mode == Mode::Probing {
            1
        } else {
            self.rounds + 1
        };

        let rate_change = self.compute_rate_change(utility_info);

        if self.direction == Direction::Increase {
            // self.sending_rate += rate_change as f64;
            self.sending_rate = self.sending_rate + rate_change;
        } else {
            self.sending_rate = QuicBandwidth::max(self.sending_rate - rate_change, QuicBandwidth::ZERO);
        }

        if self.sending_rate < QuicBandwidth::from_bits_per_second(K_MIN_SENDING_RATE )  {
            self.sending_rate = QuicBandwidth::from_bits_per_second(K_MIN_SENDING_RATE );
            self.mode = Mode::Probing;
            self.rounds = 1;
            self.incremental_rate_change_step_allowance = 0;
        } else {
            self.mode = Mode::DecisionMade;
        }
    }

    /// 处理可用的效用
    /// 参数：可用的效用信息useful_intervals，事件时间event_time
    /// 没有被调用过？？？
    /// 当monitor interval中的congestion event被调用的时候，才有可能进入这个函数
    ///
    pub fn on_utility_available(
        &mut self,
        useful_intervals: &[&MonitorInterval],
        _event_time: Instant,
    ) {
        eprintln!("enter pcc mod.rs on_utility_available");
        // 计算所有可用间隔（useful_intervals）的 utility（效用），
        // 并将这些效用信息存储在 utility_info 向量中
        let mut utility_info = Vec::new();
        for interval in useful_intervals {
            let utility = self.utility_manager.calculate_utility(interval);
            utility_info.push(UtilityInfo {
                sending_rate: interval.sending_rate,
                utility,
            });
        }

        match self.mode {
            // 处理不同模式下的效用信息
            // Starting 模式
            // 1. 如果效用信息的长度为 1，且效用大于最新的效用信息，则加倍发送速率
            // 2. 否则，直接调用 enter_probing 函数
            Mode::Starting => {
                assert!(utility_info.len() == 1);
                eprintln!(" | on_utility_available | Starting mode, utility_info[0]: {:?}", utility_info[0].utility);
                eprintln!(" | on_utility_available | latest_utility_info: {:?}", self.latest_utility_info.utility);
                if utility_info[0].utility > self.latest_utility_info.utility {
                    self.sending_rate = self.sending_rate * 2.0;
                    self.latest_utility_info = utility_info[0].clone();
                    self.rounds += 1;
                } else {
                    // pccsender的enterprobing函数，已在本类中实现
                    self.enter_probing();
                }
            }
            // Probing 模式
            // 1. 如果可以做决策（有足够数量的统计、速率变化与效用变化方向一致），则设置速率变化方向，进入决策已做出状态
            // 2. 否则，进入探测模式
            // 3. 如果 rounds 大于 1 或者当前模式是决策已做出，且发送速率小于最小发送速率，则将发送速率设置为最小发送速率
            //    并重置增量速率变化步骤允许值和 rounds
            // 4. 否则，进入探测模式
            Mode::Probing => {
                if self.can_make_decision(&utility_info) {
                    // 如果在本轮探测中没有非有用间隔，发送者需要将发送速率_改回中心速率。
                    if FLAGS_RESTORE_CENTRAL_RATE_UPON_APP_LIMITED
                        && self.monitor_queue.current().unwrap().is_useful
                    {
                        self.restore_central_sending_rate();
                    }
                    assert!(utility_info.len() == 2 * self.get_num_interval_groups_in_probing());
                    // 根据效用信息设置速率变化方向
                    self.set_rate_change_direction(&utility_info);
                    // 根据速率变化方向做速率调整，如果速率太小则重新进入探测阶段
                    self.enter_decision_made(&utility_info);
                } else {
                    self.enter_probing();
                }
                if (self.rounds > 1 || self.mode == Mode::DecisionMade)
                    && self.sending_rate <= QuicBandwidth::from_bits_per_second(K_MIN_SENDING_RATE)
                {
                    self.sending_rate = QuicBandwidth::from_bits_per_second(K_MIN_SENDING_RATE);
                    self.incremental_rate_change_step_allowance = 0;
                    self.rounds = 1;
                    self.mode = Mode::Starting;
                }
            }
            // DecisionMade 模式
            // 1. 如果效用信息的长度为 1
            // 2. 如果效用信息的效用大于最新的效用信息，且发送速率大于最新的发送速率，
            // 则进入决策已做出状态
            // 3. 否则，进入探测模式
            Mode::DecisionMade => {
                assert!(utility_info.len() == 1);
                let condition = (self.direction == Direction::Increase
                    && utility_info[0].utility > self.latest_utility_info.utility
                    && utility_info[0].sending_rate > self.latest_utility_info.sending_rate)
                    || (self.direction == Direction::Increase
                        && utility_info[0].utility < self.latest_utility_info.utility
                        && utility_info[0].sending_rate < self.latest_utility_info.sending_rate)
                    || (self.direction == Direction::Decrease
                        && utility_info[0].utility > self.latest_utility_info.utility
                        && utility_info[0].sending_rate < self.latest_utility_info.sending_rate)
                    || (self.direction == Direction::Decrease
                        && utility_info[0].utility < self.latest_utility_info.utility
                        && utility_info[0].sending_rate > self.latest_utility_info.sending_rate);
                if condition {
                    self.enter_decision_made(&utility_info);
                    self.latest_utility_info = utility_info[0].clone();
                } else {
                    self.enter_probing();
                }
            }
        }
    }

    /// 更新 RTT
    /// 参考pccsender中的update_rtt函数
    fn update_rtt(&mut self, event_time: Instant, rtt: &RttEstimator) {
        eprint!("enter fn update_rtt");
        
        let rtt_value = Duration::from_micros(rtt.get_latest().as_micros() as u64);

        // 更新 latest_rtt_
        self.latest_rtt = Duration::from_micros(rtt_value.as_micros() as u64);

        eprint!(" | update rtt | latest_rtt: {:?}", self.latest_rtt);
        // 更新 RTT 方差（rtt_deviation_）
        // us为单位
        if self.rtt_deviation.is_zero() {
            self.rtt_deviation = Duration::from_micros((self.latest_rtt.as_micros() / 2 )as u64);
        } else {
            // 用微秒为单位
            let avg_rtt_microseconds = self.avg_rtt.as_micros();
            let rtt_microseconds = rtt_value.as_micros();
            self.rtt_deviation = (self.rtt_deviation * 3 / 4) + 
                Duration::from_micros(
                    (((avg_rtt_microseconds as i128 - rtt_microseconds as i128).abs() as u128) / 4)
                        .try_into()
                        .unwrap(),
                );
        }

        // 更新 min_rtt_deviation_
        // us为单位
        if self.min_rtt_deviation.is_zero() || self.rtt_deviation < self.min_rtt_deviation {
            self.min_rtt_deviation = self.rtt_deviation;
        }

        // 更新 avg_rtt_
        self.avg_rtt = if self.avg_rtt.is_zero() {
            self.latest_rtt
        } else {
            // us为单位
            Duration::from_micros(
                ((self.avg_rtt.as_micros() as f64 * 0.875 + rtt_value.as_micros() as f64 * 0.125) as u128).try_into().unwrap(),
            )
        };
        eprint!(" | update rtt | avg_rtt: {:?}", self.avg_rtt);

        // 更新 min_rtt_
        if self.min_rtt.is_zero() || rtt_value < self.min_rtt {
            self.min_rtt = self.latest_rtt;
        }

        eprintln!(" | update rtt | min_rtt: {:?}", self.min_rtt);

        // 更新最新 ACK 时间
        self.latest_ack_timestamp = event_time;
        //self.rtt_updated = true;
    }

    /// 检查是否发生了 RTT 膨胀
    pub fn check_for_rtt_inflation(&mut self) -> bool {
        eprintln!("enter check_for_rtt_inflation");
        // 如果队列为空、没有 RTT 数据、或者当前 RTT 没有超过平均 RTT，直接返回 false
        if self.monitor_queue.is_empty()
            || self
                .monitor_queue
                .front()
                .map_or(true, |f| f.rtt_on_monitor_start.is_zero())
            || self.latest_rtt <= self.avg_rtt
        {
            eprintln!(" | check_for_rtt_inflation | monitor_queue_size:{:?} | front:{:?} | latest_rtt:{:?} | avg_rtt:{:?}", self.monitor_queue.size(), self.monitor_queue.front(), self.latest_rtt, self.avg_rtt);
            self.rtt_on_inflation_start = Some(Duration::ZERO);
            return false;
        }

        // 如果是第一次发生膨胀，记录 avg_rtt_
        if self
            .rtt_on_inflation_start
            .map_or(true, |rtt| rtt.is_zero())
        {
            eprintln!(" | check_for_rtt_inflation | rtt_on_inflation_start first time");
            self.rtt_on_inflation_start = Some(self.avg_rtt);
        }

        // 计算容忍阈值
        let max_inflation_ratio = 1.0 + self.get_max_rtt_fluctuation_tolerance();

        // 获取参考 RTT
        let rtt_on_monitor_start = if FLAGS_TRIGGER_EARLY_TERMINATION_BASED_ON_INTERVAL_QUEUE_FRONT
        {
            self.monitor_queue
                .front()
                .map_or(Duration::ZERO, |f| f.rtt_on_monitor_start)
        } else {
            self.monitor_queue
                .current()
                .map_or(Duration::ZERO, |c| c.rtt_on_monitor_start)
        };

        let inflated =
            max_inflation_ratio * (rtt_on_monitor_start.as_secs() as f64) < (self.avg_rtt.as_secs() as f64);

        let is_inflated = if !inflated && FLAGS_ENABLE_EARLY_TERMINATION_BASED_ON_LATEST_RTT_TREND {
            max_inflation_ratio
                * (self
                    .rtt_on_inflation_start
                    .unwrap_or(Duration::ZERO)
                    .as_secs() as f64)
                < (self.avg_rtt.as_secs() as f64)
        } else {
            inflated
        };

        if is_inflated {
            self.rtt_on_inflation_start = Some(Duration::ZERO);
        }

        is_inflated
    }

    /// 将当前的发送速率转换为拥塞窗口大小
    /// 返回值：拥塞窗口大小
    /// 参考pccsender中的GetCongestionWindow函数
    /// checked
    pub fn get_cwnd(&self) -> u64 {
        eprintln!("enter get_cwnd");
        let bdp_bytes = self.sending_rate.to_bytes_per_period(&self.min_rtt);
        // 设置一个最小窗口（单位：字节）为4倍的基础数据报大小
        // 4 * 1400 = 5600
        let cwnd_bytes = bdp_bytes.max(4 * BASE_DATAGRAM_SIZE);
        cwnd_bytes as u64
    }

    /// 处理拥塞事件
    pub fn pcc_on_congestion_event(
        &mut self,
        update_rtt: bool,
        rtt: &RttEstimator,
        event_time: Instant,
        acked_packets: Vec<AckedPacket>,
        lost_bytes: u64,
    ) {
        if self.latest_ack_timestamp == Instant::now() {
            self.latest_ack_timestamp = event_time;
        }

        let mut ack_interval = Duration::from_micros(0);
        if update_rtt {
            ack_interval = event_time - self.latest_ack_timestamp;
            self.update_rtt(event_time, rtt);
        }

        let avg_rtt = self.avg_rtt;

        if !self.has_seen_valid_rtt {
            self.has_seen_valid_rtt = true;
            let initial_rtt = K_INITIAL_RTT;
            if self.latest_rtt < initial_rtt {
                let gain = initial_rtt.as_micros() as f64 / self.latest_rtt.as_micros() as f64;
                self.sending_rate = self.sending_rate * gain;
            }
        }

        // 进入probe阶段的核心步骤！
        // 在最新的RTT没有超过平均值时不会进入这个if语句，第一次收到ack不会进入。
        if matches!(self.mode, Mode::Starting) && self.check_for_rtt_inflation() {
            eprintln!(" | on_congestion_event | RTT inflation detected in STARTING mode, enter PROBING");
            // 清空监控队列和统计信息
            self.monitor_queue.on_rtt_inflation_in_starting();
            // 进入探测阶段
            self.enter_probing();
            return;
        }

        self.monitor_queue.on_congestion_event(
            acked_packets.clone().into(),
            lost_bytes,
            avg_rtt,
            self.latest_rtt,
            self.min_rtt,
            event_time,
            ack_interval,
        );

    }   
}

/// PCC-Vivace 控制器的实现
impl Controller for PccVivace {
    /// 处理发送的包
    /// 参数：当前时间now，发送的字节数bytes，包号packet_number
    /// 参考pccsender中的onpacketsent函数
    /// checked
    fn on_sent(&mut self, now: Instant, bytes: u64, packet_number: u64) {
        eprintln!("call on_sent");
        // 初始化连接开始时间
        if self.conn_start_time.is_none() {
            self.conn_start_time = Some(now);
            self.latest_sent_timestamp = now;
            eprintln!(" | on_sent | conn_start_time is firstly assigned with: {:?}", self.conn_start_time);
        }

        // 创建新的监控间隔
        if self.create_new_interval(now) {
            eprintln!(" | on sent | create_new_interval");
            self.maybe_set_sending_rate();
            // 设置监控间隔持续时间为minrtt
            self.monitor_duration = self.min_rtt * 1;

            // 初始时，is useful为false
            let is_useful = self.create_useful_interval();
            self.monitor_queue.enqueue_new_monitor_interval(
                if is_useful {
                    self.sending_rate 
                } else {
                    self.get_sending_rate_for_non_useful_interval()
                },
                is_useful,
                self.get_max_rtt_fluctuation_tolerance(),
                self.avg_rtt,
            );
        }

        // 更新间隔信息
        self.monitor_queue.on_packet_sent(
            now,
            packet_number,
            bytes,
            now - self.latest_sent_timestamp,
        );
        // 返回MonitorInterval类型的所有参数
        eprintln!(" | on_sent | current monitor queue: {:?}", self.monitor_queue.current());
        self.latest_sent_timestamp = now;
    }

    /// 处理接收到的 ACK
    /// checked
    fn on_ack(
        &mut self,
        now: Instant,
        _sent: Instant,
        bytes: u64,
        _app_limited: bool,
        rtt: &RttEstimator,
    ) {
        eprintln!("call on_ack");

        // 包序号要怎么获取？每ack一次我就+1
        self.ack_packets.push(AckedPacket {
            packet_number: self.largest_packet_num_acked.unwrap_or(0) + 1,
            bytes_acked: bytes, // or event_time
            receive_timestamp: now,
        });

        // 处理ack时丢包为0
        let lost_bytes = 0;
        self.pcc_on_congestion_event(true, rtt, now, self.ack_packets.clone(), lost_bytes);

    }

    fn on_end_acks(
        &mut self,
        _now: Instant,
        _in_flight: u64,
        _app_limited: bool,
        largest_packet_num_acked: Option<u64>,
    ) {
        self.largest_packet_num_acked = largest_packet_num_acked;
    }


    /// 处理拥塞事件
    /// checked
    fn on_congestion_event(
        &mut self,
        now: Instant,
        _sent: Instant,
        _is_persistent_congestion: bool,
        lost_bytes: u64,
    ) {
        eprintln!("call on_congestion_event");

        // 空向量，长度为0
        let acked_packets_ = AckedPacketVector::new();
        // 处理拥塞事件时update rtt为false，不会更新rtt，所以rtt参数是随便写的
        let rtt_estimator = RttEstimator::new(self.avg_rtt); // Create an instance of RttEstimator
        self.pcc_on_congestion_event(false, &rtt_estimator, now, acked_packets_, lost_bytes);
    }

    fn on_mtu_update(&mut self, _new_mtu: u16) {}
    
    fn window(&self) -> u64 {
        eprintln!("call window");
        eprintln!(" | window | get cwnd calculate: {:?}", self.get_cwnd());
        //self.get_cwnd()
        // bbr2窗口：10-20000
	    return 20000;
    }

    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(self.clone())
    }

    fn initial_window(&self) -> u64 {
        // CWND是以字节数为单位，不是包个数
        self.initial_congestion_window * BASE_DATAGRAM_SIZE
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn pacing_window(&self) -> u64 {
        eprintln!("call pacing_window");
        // self.get_cwnd()
        return 20000;
    }
}

/// Configuration for the `PccVivace` congestion controller
#[derive(Debug, Clone)]
pub struct PccVivaceConfig {
    /// Initial congestion window in bytes
    pub initial_congestion_window: u64,
}

impl PccVivaceConfig {
    /// Creates a new `PccVivaceConfig` with the specified initial congestion window
    pub fn initial_window(&mut self, value: u64) -> &mut Self {
        self.initial_congestion_window = value;
        self
    }
}

impl Default for PccVivaceConfig {
    fn default() -> Self {
        Self {
            // BASE_DATAGRAM_SIZE = 1400
            initial_congestion_window: K_MAX_INITIAL_CONGESTION_WINDOW,
        }
    }
}

impl ControllerFactory for PccVivaceConfig {
    fn build(self: Arc<Self>, now: Instant, current_mtu: u16) -> Box<dyn Controller> {
        Box::new(PccVivace::new(self, now, current_mtu))
    }
}

const K_MAX_INITIAL_CONGESTION_WINDOW: u64 = 20;
