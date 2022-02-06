use rdkafka::consumer::ConsumerContext;
use std::fmt;

#[derive(Debug)]
pub enum ProcessingGuarantee {
    AtLeastOnce,
    ExactlyOnce
}

impl fmt::Display for ProcessingGuarantee {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let a = match self {
            Self::AtLeastOnce => "at_least_once",
            Self::ExactlyOnce => "exactly_once",
        };
        write!(f, "{}", a)
    }
}

#[derive(Debug)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted
}

impl fmt::Display for IsolationLevel {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let a = match self {
            Self::ReadUncommitted => "read_uncommitted",
            Self::ReadCommitted => "read_committed",
        };
        write!(f, "{}", a)
    }
}

#[derive(Debug)]
pub enum OffsetReset {
    Earliest,
    Latest
}

impl fmt::Display for OffsetReset {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let a = match self {
            Self::Earliest => "earliest",
            Self::Latest => "latest",
        };
        write!(f, "{}", a)
    }
}

pub enum PartitionAssignment {
    RangeAssignor,
    RoundRobinAssignor,
    CooperativeStickyAssignor,
    CustomAssignor(Box<dyn ConsumerContext>) // todo: implement assign
}