use rdkafka::consumer::ConsumerContext;

pub enum ProcessingGuarantee {
    AtLeastOnce,
    ExactlyOnce
}

pub enum OffsetReset {
    Earliest,
    Latest
}

pub enum PartitionAssignment {
    RangeAssignor,
    RoundRobinAssignor,
    CooperativeStickyAssignor,
    CustomAssignor(Box<dyn ConsumerContext>) // todo: implement assign
}