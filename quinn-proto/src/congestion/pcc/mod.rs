use std::any::Any;
use std::collections::VecDeque;
use std::sync::Arc;
use super::{Controller, ControllerFactory, BASE_DATAGRAM_SIZE};
use crate::connection::RttEstimator;
use crate::Duration;
use std::time::Instant;
// Define or import the missing types

mod monitor_interval_queue;
use monitor_interval_queue::{MonitorInterval, PccMonitorIntervalQueue, MyDelegate, PccMonitorIntervalQueueDelegateInterface, AckedPacket};
mod utility_manager;
use utility_manager::PccUtilityManager;

const K_INITIAL_RTT: f64 = 0.1; // seconds
const K_MEGABIT: u64 = 1024 * 1024;
const K_DECISION_MADE_STEP_SIZE: f64 = 0.02;
const K_PROBING_STEP_SIZE: f64 = 0.05;
const K_MAX_DECISION_MADE_STEP_SIZE : f64 = 0.1;
const K_UTILITY_GRADIENT_TO_RATE_CHANGE_FACTOR: f64 = 1.0;
const K_RATE_CHANGE_AMPLIFY_EXPONENT: f64 = 1.2;
const FLAGS_RESTORE_CENTRAL_RATE_UPON_APP_LIMITED: bool = false;
const K_INITIAL_MAX_STEP_SIZE: f64 = 0.05;
const K_INCREMENTAL_STEP_SIZE: f64 = 0.05;
const K_MIN_RATE_CHANGE: u64 = 500 * 1024; // bits per second
const K_MIN_SENDING_RATE: u64 = 500 * 1024; // bits per second
const K_MIN_RELIABILITY_RATIO: f64 = 0.8;

const FLAGS_ENABLE_RTT_DEVIATION_BASED_EARLY_TERMINATION: bool = true;
const FLAGS_TRIGGER_EARLY_TERMINATION_BASED_ON_INTERVAL_QUEUE_FRONT: bool =  false;
const FLAGS_ENABLE_EARLY_TERMINATION_BASED_ON_LATEST_RTT_TREND: bool =  false;
const  FLAGS_MAX_RTT_FLUCTUATION_TOLERANCE_RATIO_IN_STARTING: f64 = 100.0;
const  FLAGS_MAX_RTT_FLUCTUATION_TOLERANCE_RATIO_IN_DECISION_MADE: f64 =  1.0;
const  FLAGS_RTT_FLUCTUATION_TOLERANCE_GAIN_IN_STARTING: f64 =  2.5;
const  FLAGS_RTT_FLUCTUATION_TOLERANCE_GAIN_IN_DECISION_MADE: f64 =  1.5;
const  FLAGS_RTT_FLUCTUATION_TOLERANCE_GAIN_IN_PROBING: f64 =  1.0;
// const  FLAGS_CAN_SEND_RESPECT_CONGESTION_WINDOW: bool =  true;
// const  FLAGS_BYTES_IN_FLIGHT_GAIN: f64 =  2.5;
// const  FLAGS_EXIT_STARTING_BASED_ON_SAMPLED_BANDWIDTH: bool =  false;


/// PccVivace State Variables.
///
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
    sending_rate: f64, // bits per second
    utility: f64,
}

// 默认初始化为0
impl Default for UtilityInfo {
    fn default() -> Self {
        Self {
            sending_rate: 0.0,
            utility: 0.0,
        }
    }
}

trait FromBitsPerSecond {
    fn from_bits_per_second(value: f64) -> f64;
}

impl FromBitsPerSecond for f64 {
    fn from_bits_per_second(value: f64) -> f64 {
        value
    }
}
trait ToBps {
    fn to_bps(self) -> f64;
}

// Implement the trait for f64
impl ToBps for f64 {
    fn to_bps(self) -> f64 {
        self * 1e6 // assuming the value is in Mbps and converting to bps
    }
}

/// PccVivace 变量
///
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
    sending_rate: f64,
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
    ack_packets: VecDeque<AckedPacket>,
    largest_packet_num_acked: Option<u64>,
}



impl PccVivace {
    /// 创建新的 PccVivace 实例
    pub fn new(config: Arc<PccVivaceConfig>, _now: Instant, current_mtu: u16) -> Self {
        let initial_window: u64 = config.initial_congestion_window;
        let delegate: Arc<dyn PccMonitorIntervalQueueDelegateInterface> = Arc::new(MyDelegate);
        let monitor_queue = PccMonitorIntervalQueue::new(delegate.clone());
        
        Self {
            config,
            current_mtu: current_mtu as u64,
            conn_start_time: Some(_now),
            monitor_duration: Duration::from_secs(0),
            rtt_deviation: Duration::from_secs(0),
            min_rtt_deviation: Duration::from_secs(0),
            latest_rtt: Duration::from_secs(0),
            min_rtt: Default::default(),
            avg_rtt: Duration::from_secs(0),
            max_cwnd_bytes: 0,
            delegate,
            monitor_queue,
            utility_manager: PccUtilityManager::new(),
            minimum_rate: 1024,
            sending_rate: initial_window as f64 * 1400.0 * 8.0 / (1e6 * K_INITIAL_RTT),
            latest_utility_info: Default::default(),
            mode: Mode::Starting,
            direction: Direction::Increase,
            rounds: 1,
            incremental_rate_change_step_allowance: 0, // Provide a default value
            congestion_window: initial_window,
            initial_congestion_window: initial_window, // Use initial_window here
            latest_ack_timestamp: _now,
            latest_sent_timestamp: _now,
            rtt_updated: false,
            has_seen_valid_rtt: false,
            rtt_on_inflation_start: None,
            ack_packets: VecDeque::new(),
            largest_packet_num_acked: None,
        }
    }

    /// 创建新的监控间隔
    pub fn create_new_interval(&mut self, event_time: Instant) -> bool {
        // 如果监控间隔队列为空，则创建新的间隔
        if self.monitor_queue.is_empty() {
            return true;
        }

        // 如果没有 RTT 数据可用，返回 false
        if self.latest_rtt == Duration::ZERO {
            return false;
        }

        // 如果队列中没有有用的间隔，创建新的有用间隔
        if self.monitor_queue.num_useful_intervals() == 0 {
            return true;
        }

        // 获取当前的间隔
        let current_interval = &self.monitor_queue.current(); // 假设我们正在检查队列中的第一个间隔

        // 如果当前间隔是无用的，不创建新的间隔
        if let Some(interval) = current_interval {
            if !interval.is_useful {
                return false;
            }
        } else {
            return false;
        }
    
        // 如果当前有用间隔没有足够的 RTT 数据或者持续时间没有超过监控时间，不创建新的间隔
        if let Some(interval) = current_interval {
            if !interval.has_enough_reliable_rtt ||
                event_time - interval.first_packet_sent_time < self.monitor_duration
            {
                return false;
            }
        } else {
            return false;
        }
        // 如果当前间隔的 RTT 数据不可靠，或者持续时间没有超过监控时间，不创建新的间隔
        if let Some(current_interval) = self.monitor_queue.current() {
            let reliability_ratio = current_interval.num_reliable_rtt as f64
                / current_interval.packet_rtt_samples.len() as f64;
        
            if reliability_ratio > K_MIN_RELIABILITY_RATIO {
                return true;
            } else if current_interval.is_monitor_duration_extended {
                return true;
            } else {
                self.monitor_duration = self.monitor_duration.mul_f64(2.0);
                self.monitor_queue.extend_current_interval();
                return false;
            }
        } else {
            // current_interval 是 None，不创建新的间隔
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
            self.sending_rate *= 1.0 + K_PROBING_STEP_SIZE;
        } else {
            self.sending_rate *= 1.0 - K_PROBING_STEP_SIZE;
        }
    }

    /// Returns the current sending rate in bits per second
    pub fn get_sending_rate_for_non_useful_interval(&self) -> f64 {
        self.sending_rate
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
                            self.sending_rate = ((self.sending_rate as f64 * (1.0 / (1.0 + K_PROBING_STEP_SIZE))) as u64) as f64;
                        } else {
                            self.sending_rate = ((self.sending_rate as f64 * (1.0 / (1.0 - K_PROBING_STEP_SIZE))) as u64) as f64;
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
                    self.sending_rate = ((self.sending_rate as f64 * (1.0 / (1.0 + step))) as u64) as f64;
                } else {
                    self.sending_rate = ((self.sending_rate as f64 * (1.0 / (1.0 - step))) as u64) as f64;
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

        match self.mode {
            Mode::Starting => {
                // 当前发送速率减半
                self.sending_rate = ((self.sending_rate as f64 * 0.5) as u64) as f64;

                // 如果需要启用采样带宽退出机制（这部分注释掉了）
                /*
                if !self.bandwidth_estimate().is_zero() {
                    assert!(self.exit_starting_based_on_sampled_bandwidth);
                    self.sending_rate = self.sending_rate.min(
                        self.bandwidth_estimate() * (1.0 - K_PROBING_STEP_SIZE),
                    );
                }
                */
            }
            Mode::DecisionMade | Mode::Probing => {
                // 还原中心发送速率
                self.restore_central_sending_rate();
            }
        }

        if self.mode == Mode::Probing {
            self.rounds += 1;
            return;
        }

        self.mode = Mode::Probing;
        self.rounds = 1;

        // 不用实现hybrid
        // if self.utility_manager.get_utility_tag() == "Hybrid" {
        //     let mut effective_utility_tag = "Hybrid".to_string();

        //     let higher_probing_rate_mbps = (self.sending_rate as f64 * (1.0 + K_PROBING_STEP_SIZE)).to_bps() as f64 / K_MEGABIT as f64;
 
        //     // 从 utility 参数里取出浮点数
        //     let hybrid_switching_rate_mbps: f64 =
        //         self.utility_manager.get_utility_parameter(0).into();

        //     if higher_probing_rate_mbps > hybrid_switching_rate_mbps {
        //         effective_utility_tag = "Scavenger".to_string();
        //     }

        //     self.utility_manager
        //         .set_effective_utility_tag(effective_utility_tag);
        // }
    }

    /// 获取当前间隔数量
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

    fn create_useful_interval(&self) -> bool {
        if self.avg_rtt.as_micros() == 0 {
            // 没有 RTT 数据，说明刚开始连接，不能创建 useful interval
            assert!(self.mode == Mode::Starting);
            return false;
        }
    
        // STARTING 和 DECISION_MADE 模式下最多允许一个 useful interval
        // PROBING 模式下最多允许 2 * 组数个 useful interval
        let max_useful = if self.mode == Mode::Probing {
            2 * self.get_num_interval_groups_in_probing()
        } else {
            1
        };
    
        self.monitor_queue.num_useful_intervals() < max_useful
    }

    fn get_max_rtt_fluctuation_tolerance(&self) -> f64 {
        // 1. 基础容忍比率
        let mut tolerance_ratio = match self.mode {
            Mode::Starting => FLAGS_MAX_RTT_FLUCTUATION_TOLERANCE_RATIO_IN_STARTING,
            _ => FLAGS_MAX_RTT_FLUCTUATION_TOLERANCE_RATIO_IN_DECISION_MADE,
        };
    
        // 2. 如果启用了 RTT 偏差控制
        if FLAGS_ENABLE_RTT_DEVIATION_BASED_EARLY_TERMINATION {
            let tolerance_gain = match self.mode {
                Mode::Starting => FLAGS_RTT_FLUCTUATION_TOLERANCE_GAIN_IN_STARTING,
                Mode::Probing => FLAGS_RTT_FLUCTUATION_TOLERANCE_GAIN_IN_PROBING,
                Mode::DecisionMade => FLAGS_RTT_FLUCTUATION_TOLERANCE_GAIN_IN_DECISION_MADE,
            };
    
            let rtt_dev_us = self.rtt_deviation.as_micros() as f64;
            let avg_rtt_us = if self.avg_rtt.is_zero() {
                K_INITIAL_RTT as f64
            } else {
                self.avg_rtt.as_micros() as f64
            };
    
            let dynamic_ratio = tolerance_gain * rtt_dev_us / avg_rtt_us;
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
    pub fn compute_rate_change(&mut self, utility_info: &[UtilityInfo]) -> f64 {
        assert!(self.mode != Mode::Starting);

        let delta_sending_rate;
        let mut delta_utility = 0.0;

        if self.mode == Mode::Probing {
            delta_sending_rate = f64::max(
                utility_info[0].sending_rate,
                utility_info[1].sending_rate,
            ) - f64::min(
                utility_info[0].sending_rate,
                utility_info[1].sending_rate,
            );

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

                delta_utility += f64::max(utility_info[2 * i].utility, utility_info[2 * i + 1].utility)
                    - f64::min(utility_info[2 * i].utility, utility_info[2 * i + 1].utility);
            }
            delta_utility /= self.get_num_interval_groups_in_probing() as f64;
        } else {
            delta_sending_rate = f64::max(
                utility_info[0].sending_rate,
                self.latest_utility_info.sending_rate,
            ) - f64::min(
                utility_info[0].sending_rate,
                self.latest_utility_info.sending_rate,
            );
            delta_utility = f64::max(utility_info[0].utility, self.latest_utility_info.utility)
                - f64::min(utility_info[0].utility, self.latest_utility_info.utility);
        }

        assert!(delta_sending_rate != 0.0);

        let utility_gradient = (K_MEGABIT as f64) * delta_utility / delta_sending_rate.to_bps();
        let mut rate_change: f64 = f64::from_bits_per_second(
            utility_gradient * K_MEGABIT as f64 * K_UTILITY_GRADIENT_TO_RATE_CHANGE_FACTOR,
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
            as f64 * (K_INITIAL_MAX_STEP_SIZE + K_INCREMENTAL_STEP_SIZE * self.incremental_rate_change_step_allowance as f64);
        
        if rate_change > max_allowed_rate_change {
            rate_change = max_allowed_rate_change;
            self.incremental_rate_change_step_allowance += 1;
        } else if self.incremental_rate_change_step_allowance > 0 {
            self.incremental_rate_change_step_allowance -= 1;
        }

        f64::max(rate_change, K_MIN_RATE_CHANGE as f64)
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
        if self.mode == Mode::Probing {
            self.sending_rate = if self.direction == Direction::Increase {
                ((self.sending_rate as f64 * (1.0 + K_PROBING_STEP_SIZE)) as u64) as f64
            } else {
                ((self.sending_rate as f64 * (1.0 - K_PROBING_STEP_SIZE)) as u64) as f64
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
            self.sending_rate = self.sending_rate + rate_change as f64;

        } else {
            self.sending_rate = f64::max(self.sending_rate - rate_change, 0.0);
        }

        if self.sending_rate < (K_MIN_SENDING_RATE as u64) as f64 {
            self.sending_rate = (K_MIN_SENDING_RATE as u64) as f64;
            self.mode = Mode::Probing;
            self.rounds = 1;
            self.incremental_rate_change_step_allowance = 0;
        } else {
            self.mode = Mode::DecisionMade;
        }
    }
    
    /// 处理可用的效用
    /// 参数：可用的效用信息useful_intervals，事件时间event_time
    /// 
    pub fn on_utility_available(&mut self, useful_intervals: &[&MonitorInterval], _event_time: Instant) {
        // 计算所有可用间隔（useful_intervals）的 utility（效用），
        // 并将这些效用信息存储在 utility_info 向量中
        let mut utility_info = Vec::new();
        for interval in useful_intervals {
            let utility = self.utility_manager.calculate_utility(interval);
            utility_info.push(UtilityInfo {
                sending_rate: interval.sending_rate as f64,
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
                if utility_info[0].utility > self.latest_utility_info.utility {
                    self.sending_rate *= 2.0;
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
                    if FLAGS_RESTORE_CENTRAL_RATE_UPON_APP_LIMITED && self.monitor_queue.current().unwrap().is_useful {
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
                if (self.rounds > 1 || self.mode == Mode::DecisionMade) && self.sending_rate <= K_MIN_SENDING_RATE as f64 {
                    self.sending_rate = K_MIN_SENDING_RATE as f64;
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
                let condition = (self.direction == Direction::Increase &&
                                 utility_info[0].utility > self.latest_utility_info.utility &&
                                 utility_info[0].sending_rate > self.latest_utility_info.sending_rate) ||
                                (self.direction == Direction::Increase &&
                                 utility_info[0].utility < self.latest_utility_info.utility &&
                                 utility_info[0].sending_rate < self.latest_utility_info.sending_rate) ||
                                (self.direction == Direction::Decrease &&
                                 utility_info[0].utility > self.latest_utility_info.utility &&
                                 utility_info[0].sending_rate < self.latest_utility_info.sending_rate) ||
                                (self.direction == Direction::Decrease &&
                                 utility_info[0].utility < self.latest_utility_info.utility &&
                                 utility_info[0].sending_rate > self.latest_utility_info.sending_rate);
                if condition {
                    self.enter_decision_made(&utility_info);
                    self.latest_utility_info = utility_info[0].clone();
                } else {
                    self.enter_probing();
                }
            }
        }
    }

    fn update_rtt(&mut self, event_time: Instant, rtt: &RttEstimator,) {
        eprint!("enter fn update_rtt");
        // 参考pccsender中的update_rtt函数
        let rtt_value = rtt.latest();

        // 更新 latest_rtt_
        self.latest_rtt = rtt_value;
        eprint!("latest_rtt: {:?}", self.latest_rtt);
        // 更新 RTT 方差（rtt_deviation_）
        if self.rtt_deviation.is_zero() {
            self.rtt_deviation = rtt_value / 2;
        } else {
            let avg_rtt_microseconds = self.avg_rtt.as_micros();
            let rtt_microseconds = rtt_value.as_micros();
            self.rtt_deviation = (self.rtt_deviation * 3 / 4)
                + Duration::from_micros((((avg_rtt_microseconds as i128 - rtt_microseconds as i128).abs() as u128) / 4).try_into().unwrap());
        }

        // 更新 min_rtt_deviation_
        if self.min_rtt_deviation.is_zero() || self.rtt_deviation < self.min_rtt_deviation {
            self.min_rtt_deviation = self.rtt_deviation;
        }

        // 更新 avg_rtt_
        self.avg_rtt = if self.avg_rtt.is_zero() {
            rtt_value
        } else {
            Duration::from_secs_f64(self.avg_rtt.as_secs_f64() * 0.875 + rtt_value.as_secs_f64() * 0.125)
        };
        eprint!("avg_rtt: {:?}", self.avg_rtt);

        // 更新 min_rtt_
        if self.min_rtt.is_zero() || rtt_value < self.min_rtt {
            self.min_rtt = rtt_value;
        }
        
        eprint!("min_rtt: {:?}", self.min_rtt);

        // 更新最新 ACK 时间
        self.latest_ack_timestamp = event_time;
        self.rtt_updated = true;
    }

    /// 检查是否发生了 RTT 膨胀
    pub fn check_for_rtt_inflation(&mut self) -> bool {
        // 如果队列为空、没有 RTT 数据、或者当前 RTT 没有超过平均 RTT，直接返回 false
        if self.monitor_queue.is_empty()
            || self.monitor_queue.front().map_or(true, |f| f.rtt_on_monitor_start.is_zero())
            || self.latest_rtt <= self.avg_rtt
        {
            self.rtt_on_inflation_start = Some(Duration::from_micros(0));
            return false;
        }

        // 如果是第一次发生膨胀，记录 avg_rtt_
        if self.rtt_on_inflation_start.map_or(true, |rtt| rtt.is_zero()) {
            self.rtt_on_inflation_start = Some(self.avg_rtt);
        }

        // 计算容忍阈值
        let max_inflation_ratio = 1.0 + self.get_max_rtt_fluctuation_tolerance();

        // 获取参考 RTT
        let rtt_on_monitor_start = if FLAGS_TRIGGER_EARLY_TERMINATION_BASED_ON_INTERVAL_QUEUE_FRONT {
            self.monitor_queue.front().map_or(Duration::from_micros(0), |f| f.rtt_on_monitor_start)
        } else {
            self.monitor_queue.current().map_or(Duration::from_micros(0), |c| c.rtt_on_monitor_start)
        };

        let inflated = max_inflation_ratio * rtt_on_monitor_start.as_secs_f64() < self.avg_rtt.as_secs_f64();

        let is_inflated = if !inflated && FLAGS_ENABLE_EARLY_TERMINATION_BASED_ON_LATEST_RTT_TREND{
            max_inflation_ratio * self.rtt_on_inflation_start.unwrap_or(Duration::ZERO).as_secs_f64() < self.avg_rtt.as_secs_f64()
        } else {
            inflated
        };

        if is_inflated {
            self.rtt_on_inflation_start = Some(Duration::from_micros(0));
        }

        is_inflated
    }

}

impl Controller for PccVivace {
    fn on_sent(
        &mut self,
        now: Instant,
        bytes: u64,
        packet_number: u64,
    ) {
        // 初始化连接开始时间
        if self.conn_start_time.is_none() {
            self.conn_start_time = Some(now);
            self.conn_start_time = Some(now);
            self.latest_sent_timestamp = now;
        }

        // 创建新的监控间隔
        if self.create_new_interval(now) {
            self.maybe_set_sending_rate();
            self.monitor_duration = self.min_rtt.mul_f64(1.0);

            let is_useful = self.create_useful_interval();
            self.monitor_queue.enqueue_new_monitor_interval(
                if is_useful { self.sending_rate as u64 } else { self.get_sending_rate_for_non_useful_interval() as u64 },
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
        eprint!("on_sent: {:?}", self.monitor_queue.current());
        self.latest_sent_timestamp = now;
    }

    fn on_ack(
        &mut self,
        now: Instant,
        _sent: Instant,
        bytes: u64,
        _app_limited: bool,
        rtt: &RttEstimator,
    ) {
        
        // 更新最新 ACK 时间
        self.update_rtt(now, rtt);

        self.ack_packets.push_back(AckedPacket {
            packet_number: self.largest_packet_num_acked.unwrap_or(0) + bytes,
            bytes_acked: bytes,// or event_time
        });

        // 拆分oncongestionevent函数的一部分出来
        if self.latest_ack_timestamp == Instant::now() {
            self.latest_ack_timestamp = now;
        }

        if !self.has_seen_valid_rtt {
            self.has_seen_valid_rtt = true;
            let initial_rtt = Duration::from_micros(K_INITIAL_RTT as u64);
            if self.latest_rtt < initial_rtt {
                let gain = initial_rtt.as_micros() as f64 / self.latest_rtt.as_micros() as f64;
                self.sending_rate *= gain;
            }
        }
        eprint!("on_ack: now={:?}, bytes={}, rtt={:?}", now, bytes, self.latest_rtt);
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

    fn on_congestion_event(
        &mut self,
        now: Instant,
        _sent: Instant,
        _is_persistent_congestion: bool,
        lost_bytes: u64,
    ) {
       // 初始化 latest_ack_timestamp
        // if self.latest_ack_timestamp == Instant::now() {
        //     self.latest_ack_timestamp = now;
        // }

        // let mut ack_interval = Duration::from_micros(0);
        
        // if self.rtt_updated {
        //     ack_interval = now.duration_since(self.latest_ack_timestamp);
        //     // self.update_rtt(now, rtt);
        // }
        
        // let avg_rtt = self.avg_rtt;

        // if !self.has_seen_valid_rtt {
        //     self.has_seen_valid_rtt = true;
        //     let initial_rtt = Duration::from_micros(K_INITIAL_RTT as u64);
        //     if self.latest_rtt < initial_rtt {
        //         let gain = initial_rtt.as_micros() as f64 / self.latest_rtt.as_micros() as f64;
        //         self.sending_rate *= gain;
        //     }
        // }
        if matches!(self.mode, Mode::Starting) && self.check_for_rtt_inflation() {
            self.monitor_queue.on_rtt_inflation_in_starting();
            self.enter_probing();
            return;
        }

        let avg_rtt = self.avg_rtt;

        let mut ack_interval = Duration::from_micros(0);

        if self.rtt_updated {
            ack_interval = now.duration_since(self.latest_ack_timestamp);
        }

        self.monitor_queue.on_congestion_event(
            self.ack_packets.clone().into(),
            lost_bytes,
            avg_rtt,
            self.latest_rtt,
            self.min_rtt,
            now,
            ack_interval,
        );
        eprint!("on_congestion_event: now={:?}, lost_bytes={}, avg_rtt={:?}, min_rtt={:?}", now, lost_bytes, avg_rtt, self.min_rtt);
    }

    fn on_mtu_update(&mut self, _new_mtu: u16) {
        
    }

    fn window(&self) -> u64 {
        let window = self.get_sending_rate_for_non_useful_interval() as u64 * if self.min_rtt.is_zero() {
            K_INITIAL_RTT as u64
        } else {
            self.min_rtt.as_secs() as u64
        };
        // return 10000000;
        // self.congestion_window as u64;
        eprint!("get congestion window size: {:?}", window);
        return window
    }

    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(self.clone())
    }

    fn initial_window(&self) -> u64 {
        self.initial_congestion_window
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
    fn pacing_window(&self) -> u64{
        0
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
        Self { initial_congestion_window: K_MAX_INITIAL_CONGESTION_WINDOW * BASE_DATAGRAM_SIZE } 
    }
}

impl ControllerFactory for PccVivaceConfig {
    fn build(self: Arc<Self>, now: Instant, current_mtu: u16) -> Box<dyn Controller> {
        Box::new(PccVivace::new(self, now, current_mtu))
    }
}

const K_MAX_INITIAL_CONGESTION_WINDOW: u64 = 200;