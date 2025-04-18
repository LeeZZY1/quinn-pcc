use std::cmp::Ordering;
use std::ops::{Add, Sub, Mul};
use std::time::Duration;

// use crate::congestion::pcc::quic_time::Delta;  // Delta在quic_time.rs中定义
use crate::congestion::pcc::quic_type::QuicByteCount;  // 引入QuicByteCount类型,QuicByteCount在quic_types.rs中定义为u64

const K_NUM_MICROS_PER_SECOND: u64 = 1_000_000;  // 对应C++中的kNumMicrosPerSecond

#[derive(Clone, Debug, Copy)]
pub struct QuicBandwidth {
    bits_per_second: u64,
}

impl QuicBandwidth {
    pub const ZERO: Self = QuicBandwidth { bits_per_second: 0 };
    pub const INFINITE: Self = QuicBandwidth { bits_per_second: u64::MAX };

    pub fn new(bits_per_second: u64) -> Self {
        QuicBandwidth {
            bits_per_second: bits_per_second.max(0),  // 确保非负
        }
    }

    pub fn from_bits_per_second(bits_per_second: u64) -> Self {
        Self::new(bits_per_second)
    }

    pub fn from_k_bits_per_second(k_bits_per_second: u64) -> Self {
        Self::new(k_bits_per_second * 1000)
    }

    pub fn from_bytes_per_second(bytes_per_second: u64) -> Self {
        Self::new(bytes_per_second * 8)
    }

    pub fn from_k_bytes_per_second(k_bytes_per_second: u64) -> Self {
        Self::new(k_bytes_per_second * 8000)
    }

    pub fn from_bytes_and_time_delta(bytes: QuicByteCount, delta: &Duration) -> Self {
        let microseconds = delta.as_micros();
        if microseconds == 0 {
            Self::ZERO
        } else {
            let bits = bytes as u64 * 8 * K_NUM_MICROS_PER_SECOND as u64;
            Self::new(bits / microseconds as u64)
        }
    }

    pub fn to_bits_per_second(&self) -> u64 {
        self.bits_per_second
    }

    pub fn to_k_bits_per_second(&self) -> u64 {
        self.bits_per_second / 1000
    }

    pub fn to_bytes_per_second(&self) -> u64 {
        self.bits_per_second / 8
    }

    pub fn to_k_bytes_per_second(&self) -> u64 {
        self.bits_per_second / 8000
    }

    pub fn to_bytes_per_period(&self, time_period: &Duration) -> QuicByteCount {
        let bytes_per_second = self.to_bytes_per_second() as u64;
        let microseconds = time_period.as_micros() as u64;
        (bytes_per_second * microseconds) / K_NUM_MICROS_PER_SECOND
    }

    pub fn to_k_bytes_per_period(&self, time_period: &Duration) -> u64 {
        let k_bytes_per_second = self.to_k_bytes_per_second() as u64;
        let microseconds = time_period.as_micros() as u64;
        (k_bytes_per_second * microseconds) / K_NUM_MICROS_PER_SECOND
    }

    pub fn is_zero(&self) -> bool {
        self.bits_per_second == 0
    }

    pub fn transfer_time(&self, bytes: QuicByteCount) -> Duration {
        if self.is_zero() {
            Duration::ZERO
        } else {
            let bits = bytes as u64 * 8;
            let microseconds = (bits * K_NUM_MICROS_PER_SECOND as u64) / self.bits_per_second;
            Duration::from_micros(microseconds)
        }
    }
}

// 引入之前定义的Delta结构体（假设在同一个模块或已正确导入）
// 这里需要确保Delta结构体已经定义，包含from_microseconds等方法
// 以下是假设的Delta结构体引用示例（实际需根据模块结构调整）


// 比较运算符实现
impl PartialEq for QuicBandwidth {
    fn eq(&self, other: &Self) -> bool {
        self.bits_per_second == other.bits_per_second
    }
}

impl Eq for QuicBandwidth {}

impl PartialOrd for QuicBandwidth {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for QuicBandwidth {
    fn cmp(&self, other: &Self) -> Ordering {
        self.bits_per_second.cmp(&other.bits_per_second)
    }
}

// 算术运算符实现
impl Add for QuicBandwidth {
    type Output = Self;
    fn add(self, other: Self) -> Self::Output {
        Self::new(self.bits_per_second + other.bits_per_second)
    }
}

impl Sub for QuicBandwidth {
    type Output = Self;
    fn sub(self, other: Self) -> Self::Output {
        Self::new(self.bits_per_second - other.bits_per_second)
    }
}

impl Mul<f64> for QuicBandwidth {
    type Output = Self;
    fn mul(self, factor: f64) -> Self::Output {
        let bits = (self.bits_per_second as f64 * factor).round() as u64;
        Self::new(bits)
    }
}

// 带宽与时间间隔的乘法（返回字节数）
impl Mul<&Duration> for QuicBandwidth {
    type Output = QuicByteCount;
    fn mul(self, delta: &Duration) -> Self::Output {
        self.to_bytes_per_period(delta)
    }
}
