use std::cmp::Ordering;
use std::ops::{Add, Sub, Mul, Shl, Shr};
use std::time::{Duration, Instant};

// C++ 的class QuicTime转换为 Rust 的pub struct QuicTime，成员变量time_改为time

#[derive(Clone, Debug, Copy)]
pub struct QuicTime {
    time: u64,
}

impl QuicTime {
    // 静态工厂方法（如Zero()）转换为关联函数（fn zero() -> Self）
    pub fn zero() -> Self {
        QuicTime { time: 0 }
    }

    pub fn infinite() -> Self {
        QuicTime { time: Delta::K_QUIC_INFINITE_TIME_US }  // 修正常量引用
    }

    pub fn is_initialized(&self) -> bool {
        self.time != 0
    }

    // 需要修改
    pub fn to_instant(&self) -> Instant {
        // Replace this with the actual conversion logic if needed
        Instant::now()
    }

    // 时间运算
    pub fn add_delta(&self, delta: &Delta) -> QuicTime {
        QuicTime { time: self.time + delta.time_offset }
    }

    pub fn sub_delta(&self, delta: &Delta) -> QuicTime {
        QuicTime { time: self.time - delta.time_offset }
    }
}

#[derive(Clone, Debug, Copy)]
pub struct Delta {
    pub time_offset: u64,
}

impl Delta {
    pub const K_QUIC_INFINITE_TIME_US: u64 = u64::MAX;  // 保持常量命名规范

    pub fn new(time_offset: u64) -> Self {
        Delta { time_offset }
    }

    pub fn zero() -> Self {
        Delta { time_offset: 0 }
    }

    pub fn infinite() -> Self {
        Delta { time_offset: Self::K_QUIC_INFINITE_TIME_US }
    }

    pub fn from_seconds(secs: u64) -> Self {
        Delta { time_offset: secs * 1_000_000 }
    }

    pub fn from_milliseconds(ms: u64) -> Self {
        Delta { time_offset: ms * 1_000 }
    }

    pub const fn from_microseconds(us: u64) -> Self {
        Delta { time_offset: us }
    }

    pub fn to_seconds(&self) -> u64 {
        self.time_offset / 1_000_000
    }

    pub fn to_milliseconds(&self) -> u64 {
        self.time_offset / 1_000
    }

    pub fn to_microseconds(&self) -> u64 {
        self.time_offset
    }

    pub fn is_zero(&self) -> bool {
        self.time_offset == 0
    }

    pub fn is_infinite(&self) -> bool {
        self.time_offset == Self::K_QUIC_INFINITE_TIME_US
    }
}

// 比较运算符通过实现PartialEq、Eq、PartialOrd、OrdTrait 实现
// Delta 比较运算符实现
impl PartialEq for Delta {
    fn eq(&self, other: &Self) -> bool {
        self.time_offset == other.time_offset
    }
}

impl Eq for Delta {}

impl PartialOrd for Delta {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Delta {
    fn cmp(&self, other: &Self) -> Ordering {
        self.time_offset.cmp(&other.time_offset)
    }
}

// Delta 算术运算符实现
impl Add for Delta {
    type Output = Self;
    fn add(self, other: Self) -> Self::Output {
        Delta { time_offset: self.time_offset + other.time_offset }
    }
}

impl Sub for Delta {
    type Output = Self;
    fn sub(self, other: Self) -> Self::Output {
        Delta { time_offset: self.time_offset - other.time_offset }
    }
}

impl Mul<u64> for Delta {
    type Output = Self;
    fn mul(self, rhs: u64) -> Self::Output {
        Delta { time_offset: self.time_offset * rhs }
    }
}

impl Mul<f64> for Delta {
    type Output = Self;
    fn mul(self, rhs: f64) -> Self::Output {
        Delta { time_offset: (self.time_offset as f64 * rhs).round() as u64 }
    }
}

// QuicTime 比较运算符实现
impl PartialEq for QuicTime {
    fn eq(&self, other: &Self) -> bool {
        self.time == other.time
    }
}

impl Eq for QuicTime {}

impl PartialOrd for QuicTime {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for QuicTime {
    fn cmp(&self, other: &Self) -> Ordering {
        self.time.cmp(&other.time)
    }
}

// QuicTime 与 Delta 算术运算
impl Add<Delta> for QuicTime {
    type Output = Self;
    fn add(self, delta: Delta) -> Self::Output {
        QuicTime { time: self.time + delta.time_offset }
    }
}

impl Sub<Delta> for QuicTime {
    type Output = Self;
    fn sub(self, delta: Delta) -> Self::Output {
        QuicTime { time: self.time - delta.time_offset }
    }
}

impl Sub for QuicTime {
    type Output = Delta;
    fn sub(self, other: QuicTime) -> Self::Output {
        Delta { time_offset: self.time - other.time }
    }
}

// 位移运算符实现（仅示例，实际需根据需求调整）
impl Shl<usize> for Delta {
    type Output = Self;
    fn shl(self, rhs: usize) -> Self::Output {
        Delta { time_offset: self.time_offset << rhs }
    }
}

impl Shr<usize> for Delta {
    type Output = Self;
    fn shr(self, rhs: usize) -> Self::Output {
        Delta { time_offset: self.time_offset >> rhs }
    }
}
