use crate::congestion::pcc::monitor_interval_queue::MonitorInterval;
use std::collections::VecDeque;
use std::f64;
use std::sync::Arc;
use std::time::{Duration, Instant};
// use crate::congestion::pcc::quic_time::Delta;
const K_MAX_PACKET_SIZE: u64 = 1500;
const K_BITS_PER_BYTE: usize = 8;
const K_RTT_HISTORY_LEN: usize = 6;
const K_LOSS_TOLERANCE: f64 = 0.05;
const K_LOSS_COEFFICIENT: f64 = -1000.0;
const K_RTT_COEFFICIENT: f64 = -200.0;
const K_SENDING_RATE_EXPONENT: f64 = 0.9;
const K_VIVACE_LOSS_COEFFICIENT: f64 = 11.35;
const K_LATENCY_COEFFICIENT: f64 = 900.0;
const K_RTT_DEVIATION_COEFFICIENT: f64 = 0.0015;
const K_HYBRID_UTILITY_RATE_TRANSFORM_FACTOR: f64 = 0.1;
const K_ALPHA: f64 = 0.1;
const K_BETA: f64 = 100.0;
const K_TRENDING_RESET_INTERVAL_RATIO: f64 = 0.95;
const K_INFLATION_TOLERANCE_GAIN_HIGH: f64 = 2.0;
const K_INFLATION_TOLERANCE_GAIN_LOW: f64 = 2.0;
const K_LOST_PACKET_TOLERANCE: usize = 10;
const K_NUM_MICROS_PER_SECOND: u64 = 1_000_000;

#[derive(Clone)]
pub struct IntervalStats {
    marked_lost_bytes: u64,
    interval_duration: f64,
    rtt_ratio: f64,
    loss_rate: f64,
    actual_sending_rate_mbps: f64,
    ack_rate_mbps: f64,
    approx_rtt_gradient: f64,
    rtt_gradient: f64,
    rtt_gradient_cut: f64,
    rtt_gradient_error: f64,
    rtt_dev: f64,
    max_rtt: f64,
    min_rtt: f64,
    avg_rtt: f64,
    trending_gradient: f64,
    trending_gradient_cut: f64,
    trending_gradient_error: f64,
    trending_deviation: f64,
}

impl Default for IntervalStats {
    fn default() -> Self {
        Self {
            marked_lost_bytes: 0,
            interval_duration: 0.0,
            rtt_ratio: 0.0,
            loss_rate: 0.0,
            actual_sending_rate_mbps: 0.0,
            ack_rate_mbps: 0.0,
            approx_rtt_gradient: 0.0,
            rtt_gradient: 0.0,
            rtt_gradient_cut: 0.0,
            rtt_gradient_error: 0.0,
            rtt_dev: 0.0,
            max_rtt: 0.0,
            min_rtt: 0.0,
            avg_rtt: 0.0,
            trending_gradient: 0.0,
            trending_gradient_cut: 0.0,
            trending_gradient_error: 0.0,
            trending_deviation: 0.0,
        }
    }
}
#[derive(Clone)]
#[allow(dead_code)]
pub(super) struct PccUtilityManager {
    utility_tag: String,
    effective_utility_tag: String,
    lost_bytes_tolerance_quota: u64,
    avg_mi_rtt_dev: f64,
    dev_mi_rtt_dev: f64,
    min_rtt: f64,
    avg_trending_gradient: f64,
    min_trending_gradient: f64,
    dev_trending_gradient: f64,
    last_trending_gradient: f64,
    avg_trending_dev: f64,
    min_trending_dev: f64,
    dev_trending_dev: f64,
    last_trending_dev: f64,
    ratio_inflated_mi: f64,
    ratio_fluctuated_mi: f64,
    is_rtt_inflation_tolerable: bool,
    is_rtt_dev_tolerable: bool,
    mi_avg_rtt_history: VecDeque<f64>,
    mi_rtt_dev_history: VecDeque<f64>,
    interval_stats: IntervalStats,
    utility_parameters: Vec<f32>,
    bits_per_second: u64,
}

impl PccUtilityManager {
    pub(super) fn new() -> Self {
        Self {
            utility_tag: "Allegro".to_string(),
            effective_utility_tag: "Allegro".to_string(),
            lost_bytes_tolerance_quota: K_MAX_PACKET_SIZE * K_LOST_PACKET_TOLERANCE as u64,
            avg_mi_rtt_dev: -1.0,
            dev_mi_rtt_dev: -1.0,
            min_rtt: -1.0,
            avg_trending_gradient: -1.0,
            min_trending_gradient: -1.0,
            dev_trending_gradient: -1.0,
            last_trending_gradient: -1.0,
            avg_trending_dev: -1.0,
            min_trending_dev: -1.0,
            dev_trending_dev: -1.0,
            last_trending_dev: -1.0,
            ratio_inflated_mi: 0.0,
            ratio_fluctuated_mi: 0.0,
            is_rtt_inflation_tolerable: true,
            is_rtt_dev_tolerable: true,
            mi_avg_rtt_history: VecDeque::with_capacity(K_RTT_HISTORY_LEN),
            mi_rtt_dev_history: VecDeque::with_capacity(K_RTT_HISTORY_LEN),
            interval_stats: IntervalStats::default(),
            utility_parameters: Vec::new(),
            bits_per_second: 0,
        }
    }

    pub(super) fn get_utility_tag(&self) -> &str {
        &self.utility_tag
    }

    pub(super) fn get_utility_parameter(&self, parameter_index: usize) -> f32 {
        if parameter_index < self.utility_parameters.len() {
            self.utility_parameters[parameter_index]
        } else {
            0.0
        }
    }

    pub(super) fn get_effective_utility_tag(&self) -> &str {
        &self.effective_utility_tag
    }

    pub(super) fn set_utility_tag(&mut self, utility_tag: String) {
        self.utility_tag = utility_tag.clone();
        self.effective_utility_tag = utility_tag.clone();
        println!("Using Utility Function: {}", self.utility_tag);
    }

    pub(super) fn set_effective_utility_tag(&mut self, utility_tag: String) {
        self.effective_utility_tag = utility_tag;
    }

    pub(super) fn set_utility_parameter(&mut self, param: f32) {
        self.utility_parameters.push(param);
        println!("Update Utility Parameter: {}", param);
    }

    pub(super) fn transfer_time(&self, bytes: u64, bits_per_second: u64) -> Duration {
        if bits_per_second == 0 {
            return Duration::ZERO;
        }
        let microseconds = bytes * 8 * 1_000_000 / bits_per_second;
        Duration::from_micros(microseconds)
        // Delta::from_microseconds(microseconds as u64)
    }

    pub(super) fn calculate_utility(&mut self, interval: &MonitorInterval) -> f64 {
        assert!(interval.first_packet_sent_time != interval.last_packet_sent_time);
        self.prepare_statistics(interval);

        match self.effective_utility_tag.as_str() {
            "Allegro" => self.calculate_utility_allegro(interval),
            "Vivace" => self.calculate_utility_vivace(interval),
            "Proportional" => {
                let latency_coeff = self.utility_parameters[0] as f64;
                let loss_coeff = self.utility_parameters[1] as f64;
                self.calculate_utility_proportional(interval, latency_coeff, loss_coeff)
            }
            "Scavenger" => {
                let coeff = self.utility_parameters[0] as f64;
                self.calculate_utility_scavenger(interval, coeff)
            }
            "HybridAllegro" => {
                let bound = self.utility_parameters[0] as f64;
                self.calculate_utility_hybrid_allegro(interval, bound)
            }
            _ => {
                // 处理所有其他情况，或者记录错误
                panic!("Unknown utility tag: {}", self.effective_utility_tag);
            }
        }
    }

    fn calculate_utility_allegro(&self, interval: &MonitorInterval) -> f64 {
        let mut rtt_ratio = self.interval_stats.rtt_ratio;
        if rtt_ratio > 1.0 - interval.rtt_fluctuation_tolerance_ratio
            && rtt_ratio < 1.0 + interval.rtt_fluctuation_tolerance_ratio
        {
            rtt_ratio = 1.0;
        }

        let latency_penalty = 1.0 - 1.0 / (1.0 + f64::exp(K_RTT_COEFFICIENT * (1.0 - rtt_ratio)));
        let loss_penalty = 1.0
            - 1.0
                / (1.0
                    + f64::exp(
                        K_LOSS_COEFFICIENT * (self.interval_stats.loss_rate - K_LOSS_TOLERANCE),
                    ));
        let sending_contribution = (interval.bytes_acked as f64 * 8.0)
            / self.interval_stats.interval_duration
            * loss_penalty
            * latency_penalty;
        let loss_contribution =
            (interval.bytes_lost as f64 * 8.0) / self.interval_stats.interval_duration;
        (sending_contribution - loss_contribution) * 1000.0
    }

    fn calculate_utility_vivace(&self, interval: &MonitorInterval) -> f64 {
        self.calculate_utility_proportional(
            interval,
            K_LATENCY_COEFFICIENT,
            K_VIVACE_LOSS_COEFFICIENT,
        )
    }

    fn calculate_utility_proportional(
        &self,
        interval: &MonitorInterval,
        latency_coeff: f64,
        loss_coeff: f64,
    ) -> f64 {
        let sending_contribution = f64::powf(
            self.interval_stats.actual_sending_rate_mbps,
            K_SENDING_RATE_EXPONENT,
        );
        let rtt_gradient = if self.is_rtt_inflation_tolerable {
            0.0
        } else {
            self.interval_stats.rtt_gradient
        };
        let latency_penalty =
            latency_coeff * rtt_gradient * self.interval_stats.actual_sending_rate_mbps;
        let loss_penalty = loss_coeff
            * self.interval_stats.loss_rate
            * self.interval_stats.actual_sending_rate_mbps;
        sending_contribution - latency_penalty - loss_penalty
    }

    fn calculate_utility_scavenger(&self, interval: &MonitorInterval, rtt_dev_coeff: f64) -> f64 {
        let sending_contribution = f64::powf(
            self.interval_stats.actual_sending_rate_mbps,
            K_SENDING_RATE_EXPONENT,
        );
        let latency_penalty = K_LATENCY_COEFFICIENT
            * self.interval_stats.rtt_gradient
            * self.interval_stats.actual_sending_rate_mbps;
        let loss_penalty = K_VIVACE_LOSS_COEFFICIENT
            * self.interval_stats.loss_rate
            * self.interval_stats.actual_sending_rate_mbps;
        let rtt_dev_penalty = rtt_dev_coeff
            * self.interval_stats.rtt_dev
            * self.interval_stats.actual_sending_rate_mbps;
        sending_contribution - latency_penalty - loss_penalty - rtt_dev_penalty
    }

    fn calculate_utility_hybrid_allegro(&self, interval: &MonitorInterval, bound: f64) -> f64 {
        if self.interval_stats.actual_sending_rate_mbps < bound {
            self.calculate_utility_allegro(interval)
        } else {
            let allegro_utility = self.calculate_utility_allegro(interval);
            let perfect = self.calculate_perfect_utility_allegro(bound);
            let bounded = bound
                + (self.interval_stats.actual_sending_rate_mbps - bound)
                    * K_HYBRID_UTILITY_RATE_TRANSFORM_FACTOR;
            let bounded_perfect = self.calculate_perfect_utility_allegro(bounded);
            bounded_perfect * (allegro_utility / perfect)
        }
    }

    fn calculate_perfect_utility_allegro(&self, sending_rate: f64) -> f64 {
        let latency_penalty = 1.0 - 1.0 / (1.0 + f64::exp(K_RTT_COEFFICIENT * (1.0 - 1.0)));
        let loss_penalty =
            1.0 - 1.0 / (1.0 + f64::exp(K_LOSS_COEFFICIENT * (0.0 - K_LOSS_TOLERANCE)));
        (sending_rate * 1e6 / 8.0) * loss_penalty * latency_penalty * 1000.0
    }

    fn calculate_perfect_utility_vivace(&self, sending_rate: f64) -> f64 {
        f64::powf(sending_rate, K_SENDING_RATE_EXPONENT)
    }

    fn prepare_statistics(&mut self, interval: &MonitorInterval) {
        self.pre_processing(interval);
        self.compute_simple_metrics(interval);
        self.compute_approx_rtt_gradient(interval);
        self.compute_rtt_gradient(interval);
        self.compute_rtt_deviation(interval);
        self.compute_rtt_gradient_error(interval);
        self.determine_tolerance_general();
        self.process_rtt_trend(interval);
    }

    fn pre_processing(&mut self, _interval: &MonitorInterval) {
        self.interval_stats.marked_lost_bytes = 0;
        // 原C++代码中的标记丢失字节逻辑较为复杂，这里暂未实现
    }

    // fn compute_simple_metrics(&mut self, interval: &MonitorInterval) {
    //     let duration = interval.last_packet_sent_time
    //                     .duration_since(interval.first_packet_sent_time)
    //                     .unwrap_or_else(|_| Duration::ZERO);
    //     self.interval_stats.interval_duration = duration.as_micros() as f64;

    //     let start_rtt = interval.rtt_on_monitor_start.as_micros() as f64;
    //     let end_rtt = interval.rtt_on_monitor_end.as_micros() as f64;
    //     self.interval_stats.rtt_ratio = if end_rtt != 0.0 { start_rtt / end_rtt } else { 1.0 };

    //     let total_bytes = interval.bytes_sent as f64;
    //     self.interval_stats.loss_rate = if total_bytes > 0.0 { (interval.bytes_lost as f64) / total_bytes } else { 0.0 };

    //     self.interval_stats.actual_sending_rate_mbps = (interval.bytes_sent as f64 * 8.0) / self.interval_stats.interval_duration;

    //     let num_rtt_samples = interval.packet_rtt_samples.len();
    //     if num_rtt_samples >= 1 {
    //         let first_ack = interval.packet_rtt_samples[0].ack_timestamp;
    //         let last_ack = interval.packet_rtt_samples[num_rtt_samples - 1].ack_timestamp;
    //         let duration = interval.last_packet_sent_time.saturating_duration_since(interval.first_packet_sent_time);
    //         self.interval_stats.ack_rate_mbps = (interval.bytes_acked as f64 * 8.0) / duration.as_micros() as f64;
    //     }
    // }

    pub(super) fn compute_simple_metrics(&mut self, interval: &MonitorInterval) {
        // Add the transfer time of the last packet in the monitor interval when
        // calculating monitor interval duration.

        let transfer_time = self.transfer_time(K_MAX_PACKET_SIZE, interval.sending_rate.to_bits_per_second());
        self.interval_stats.interval_duration =
            ((interval.last_packet_sent_time - interval.first_packet_sent_time + transfer_time).as_micros())
                as f64 / 1_000_000.0;

        self.interval_stats.rtt_ratio = (interval.rtt_on_monitor_start.as_micros() as f64)
            / (interval.rtt_on_monitor_end.as_micros() as f64);

        self.interval_stats.loss_rate = (interval.bytes_lost
            - self.interval_stats.marked_lost_bytes) as f64
            / interval.bytes_sent as f64;

        self.interval_stats.actual_sending_rate_mbps = (interval.bytes_sent as f64
            * K_BITS_PER_BYTE as f64)
            / self.interval_stats.interval_duration;

        let num_rtt_samples = interval.packet_rtt_samples.len();
        if num_rtt_samples > 1 {
            let ack_duration = (interval.packet_rtt_samples[num_rtt_samples - 1].ack_timestamp
                - interval.packet_rtt_samples[0].ack_timestamp)
                .as_micros() as f64;

            self.interval_stats.ack_rate_mbps =
                (interval.bytes_acked as f64 - K_MAX_PACKET_SIZE as f64) * K_BITS_PER_BYTE as f64
                    / ack_duration;
        } else if num_rtt_samples == 1 {
            self.interval_stats.ack_rate_mbps =
                interval.bytes_acked as f64 / self.interval_stats.interval_duration;
        } else {
            self.interval_stats.ack_rate_mbps = 0.0;
        }
    }

    fn compute_approx_rtt_gradient(&mut self, interval: &MonitorInterval) {
        let samples = &interval.packet_rtt_samples;
        let count = samples.len();
        if count < 2 {
            self.interval_stats.approx_rtt_gradient = 0.0;
            return;
        }

        let half = count / 2;
        let (first_half, second_half) = samples.split_at(half);

        let (mut sum_first, mut count_first) = (0.0, 0);
        let (mut sum_second, mut count_second) = (0.0, 0);

        for sample in first_half {
            if sample.is_reliable_for_gradient_calculation {
                sum_first += sample.sample_rtt.as_micros() as f64;
                count_first += 1;
            }
        }

        for sample in second_half {
            if sample.is_reliable_for_gradient_calculation {
                sum_second += sample.sample_rtt.as_micros() as f64;
                count_second += 1;
            }
        }

        if count_first == 0 || count_second == 0 {
            self.interval_stats.approx_rtt_gradient = 0.0;
            return;
        }

        let avg_first = sum_first / count_first as f64;
        let avg_second = sum_second / count_second as f64;
        self.interval_stats.approx_rtt_gradient =
            2.0 * (avg_second - avg_first) / (avg_second + avg_first);
    }

    fn compute_rtt_gradient(&mut self, interval: &MonitorInterval) {
        let samples = &interval.packet_rtt_samples;
        let count = samples.len();
        if count < 2 {
            self.interval_stats.rtt_gradient = 0.0;
            return;
        }

        let mut sum_x = 0.0;
        let mut sum_y = 0.0;
        let mut sum_xx = 0.0;
        let mut sum_xy = 0.0;
        let mut reliable_count = 0;

        for sample in samples {
            if !sample.is_reliable_for_gradient_calculation {
                continue;
            }
            let x = sample.packet_number as f64;
            let y = sample.sample_rtt.as_micros() as f64;
            sum_x += x;
            sum_y += y;
            sum_xx += x * x;
            sum_xy += x * y;
            reliable_count += 1;
        }

        if reliable_count < 2 {
            self.interval_stats.rtt_gradient = 0.0;
            return;
        }

        let x_avg = sum_x / reliable_count as f64;
        let y_avg = sum_y / reliable_count as f64;
        let numerator = sum_xy - x_avg * sum_x;
        let denominator = sum_xx - x_avg * sum_x;
        self.interval_stats.rtt_gradient = numerator / denominator;
        self.interval_stats.avg_rtt = y_avg;
        self.interval_stats.rtt_gradient_cut = y_avg - self.interval_stats.rtt_gradient * x_avg;
    }

    fn compute_rtt_gradient_error(&mut self, interval: &MonitorInterval) {
        let samples = &interval.packet_rtt_samples;
        let count = samples.len();
        if count < 2 {
            self.interval_stats.rtt_gradient_error = 0.0;
            return;
        }

        let mut error_sum = 0.0;
        let mut reliable_count = 0;

        for sample in samples {
            if !sample.is_reliable_for_gradient_calculation {
                continue;
            }
            let x = sample.packet_number as f64;
            let y = sample.sample_rtt.as_micros() as f64;
            let pred = self.interval_stats.rtt_gradient * x + self.interval_stats.rtt_gradient_cut;
            error_sum += (y - pred).powi(2);
            reliable_count += 1;
        }

        if reliable_count < 2 {
            self.interval_stats.rtt_gradient_error = 0.0;
            return;
        }

        self.interval_stats.rtt_gradient_error = (error_sum / reliable_count as f64).sqrt();
        self.interval_stats.rtt_gradient_error /= self.interval_stats.avg_rtt;
    }

    fn compute_rtt_deviation(&mut self, interval: &MonitorInterval) {
        let samples = &interval.packet_rtt_samples;
        let count = samples.len();
        if count < 2 {
            self.interval_stats.rtt_dev = 0.0;
            return;
        }

        let mut sum = 0.0;
        let mut reliable_count = 0;
        let mut min_rtt = f64::MAX;
        let mut max_rtt = f64::MIN;

        for sample in samples {
            if !sample.is_reliable {
                continue;
            }
            let rtt = sample.sample_rtt.as_micros() as f64;
            sum += (rtt - self.interval_stats.avg_rtt).powi(2);
            reliable_count += 1;
            if rtt < min_rtt {
                min_rtt = rtt;
            }
            if rtt > max_rtt {
                max_rtt = rtt;
            }
        }

        if reliable_count < 2 {
            self.interval_stats.rtt_dev = 0.0;
            return;
        }

        self.interval_stats.rtt_dev = (sum / reliable_count as f64).sqrt();
        self.interval_stats.min_rtt = min_rtt;
        self.interval_stats.max_rtt = max_rtt;
    }

    fn process_rtt_trend(&mut self, interval: &MonitorInterval) {
        if interval.num_reliable_rtt < 2 {
            return;
        }

        self.mi_avg_rtt_history
            .push_back(self.interval_stats.avg_rtt);
        self.mi_rtt_dev_history
            .push_back(self.interval_stats.rtt_dev);

        if self.mi_avg_rtt_history.len() > K_RTT_HISTORY_LEN {
            self.mi_avg_rtt_history.pop_front();
        }
        if self.mi_rtt_dev_history.len() > K_RTT_HISTORY_LEN {
            self.mi_rtt_dev_history.pop_front();
        }

        if self.mi_avg_rtt_history.len() >= K_RTT_HISTORY_LEN {
            self.compute_trending_gradient();
            self.compute_trending_gradient_error();
            self.determine_tolerance_inflation();
        }

        if self.mi_rtt_dev_history.len() >= K_RTT_HISTORY_LEN {
            self.compute_trending_deviation();
            self.determine_tolerance_deviation();
        }
    }

    fn compute_trending_gradient(&mut self) {
        let samples = &self.mi_avg_rtt_history;
        let count = samples.len();
        if count < 2 {
            return;
        }

        let mut sum_x = 0.0;
        let mut sum_y = 0.0;
        let mut sum_xx = 0.0;
        let mut sum_xy = 0.0;

        for (i, &y) in samples.iter().enumerate() {
            let x = i as f64;
            sum_x += x;
            sum_y += y;
            sum_xx += x * x;
            sum_xy += x * y;
        }

        let x_avg = sum_x / count as f64;
        let y_avg = sum_y / count as f64;
        let numerator = sum_xy - x_avg * sum_x;
        let denominator = sum_xx - x_avg * sum_x;
        self.interval_stats.trending_gradient = numerator / denominator;
        self.interval_stats.trending_gradient_cut =
            y_avg - self.interval_stats.trending_gradient * x_avg;
    }

    fn compute_trending_gradient_error(&mut self) {
        let samples = &self.mi_avg_rtt_history;
        let count = samples.len();
        if count < 2 {
            return;
        }

        let mut error_sum = 0.0;
        for (i, &y) in samples.iter().enumerate() {
            let x = i as f64;
            let pred = self.interval_stats.trending_gradient * x
                + self.interval_stats.trending_gradient_cut;
            error_sum += (y - pred).powi(2);
        }

        self.interval_stats.trending_gradient_error = (error_sum / count as f64).sqrt();
    }

    fn compute_trending_deviation(&mut self) {
        let samples = &self.mi_rtt_dev_history;
        let count = samples.len();
        if count < 2 {
            return;
        }

        let avg = samples.iter().sum::<f64>() / count as f64;
        let mut sum = 0.0;
        for &y in samples {
            sum += (y - avg).powi(2);
        }
        self.interval_stats.trending_deviation = (sum / count as f64).sqrt();
    }

    fn determine_tolerance_general(&mut self) {
        self.is_rtt_inflation_tolerable =
            self.interval_stats.rtt_gradient_error >= self.interval_stats.rtt_gradient.abs();
        self.is_rtt_dev_tolerable = self.is_rtt_inflation_tolerable;
    }

    fn determine_tolerance_inflation(&mut self) {
        if self.utility_tag != "Scavenger" && self.mi_avg_rtt_history.len() < K_RTT_HISTORY_LEN {
            return;
        }

        let trending_gradient = self.interval_stats.trending_gradient;
        let trending_error = self.interval_stats.trending_gradient_error;

        if self.min_trending_gradient < 1e-6
            || trending_gradient.abs() < self.min_trending_gradient / K_BETA
        {
            self.avg_trending_gradient = 0.0;
            self.min_trending_gradient = trending_gradient.abs();
            self.dev_trending_gradient = trending_gradient.abs();
            self.last_trending_gradient = trending_gradient;
        } else {
            let dev_gain = if self.interval_stats.rtt_dev < 1000.0 {
                K_INFLATION_TOLERANCE_GAIN_LOW
            } else {
                K_INFLATION_TOLERANCE_GAIN_HIGH
            };

            let threshold_high = self.avg_trending_gradient + dev_gain * self.dev_trending_gradient;
            let threshold_low = self.avg_trending_gradient - dev_gain * self.dev_trending_gradient;

            if trending_gradient < threshold_low || trending_gradient > threshold_high {
                if trending_gradient > 0.0 {
                    self.is_rtt_inflation_tolerable = false;
                }
                self.is_rtt_dev_tolerable = false;
                self.ratio_inflated_mi += K_ALPHA;
            } else {
                self.dev_trending_gradient = self.dev_trending_gradient * (1.0 - K_ALPHA)
                    + (trending_gradient - self.last_trending_gradient).abs() * K_ALPHA;
                self.avg_trending_gradient =
                    self.avg_trending_gradient * (1.0 - K_ALPHA) + trending_gradient * K_ALPHA;
                self.last_trending_gradient = trending_gradient;
            }

            if trending_gradient.abs() < self.min_trending_gradient {
                self.min_trending_gradient = trending_gradient.abs();
            }
        }

        if self.ratio_inflated_mi > K_TRENDING_RESET_INTERVAL_RATIO {
            self.avg_trending_gradient = 0.0;
            self.dev_trending_gradient = self.min_trending_gradient;
            self.ratio_inflated_mi = 0.0;
        }
    }

    fn determine_tolerance_deviation(&mut self) {
        if self.avg_mi_rtt_dev < 1e-6 {
            self.avg_mi_rtt_dev = self.interval_stats.rtt_dev;
            self.dev_mi_rtt_dev = 0.5 * self.interval_stats.rtt_dev;
        } else {
            if self.interval_stats.rtt_dev > self.avg_mi_rtt_dev + 4.0 * self.dev_mi_rtt_dev
                && self.interval_stats.rtt_dev > 1000.0
            {
                self.is_rtt_dev_tolerable = false;
                self.ratio_fluctuated_mi += K_ALPHA;
            } else {
                self.dev_mi_rtt_dev = self.dev_mi_rtt_dev * (1.0 - K_ALPHA)
                    + (self.interval_stats.rtt_dev - self.avg_mi_rtt_dev).abs() * K_ALPHA;
                self.avg_mi_rtt_dev =
                    self.avg_mi_rtt_dev * (1.0 - K_ALPHA) + self.interval_stats.rtt_dev * K_ALPHA;
            }
        }

        if self.ratio_fluctuated_mi > K_TRENDING_RESET_INTERVAL_RATIO {
            self.avg_mi_rtt_dev = -1.0;
            self.dev_mi_rtt_dev = -1.0;
            self.ratio_fluctuated_mi = 0.0;
        }
    }
}
