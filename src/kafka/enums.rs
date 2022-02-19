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


///
/// Security Protocol
#[derive(Debug)]
pub enum SecurityProtocol {
    Plaintext,
    Ssl,
    SaslPlaintext,
    SaslSsl
}

impl fmt::Display for SecurityProtocol {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let a = match self {
            Self::Plaintext => "PLAINTEXT",
            Self::Ssl => "SSL",
            Self::SaslPlaintext => "SASL_PLAINTEXT",
            Self::SaslSsl => "SASL_SSL",
        };
        write!(f, "{}", a)
    }
}

///
/// SASL Mechanism to use
/// Possible values are:
/// * GSSAPI
/// * Plain
/// * SCRAM-SHA-256
/// * SCRAM-SHA-512
/// * OAUTHBEARER
#[derive(Debug, Copy, Clone)]
pub enum SaslMechanism {
    GssAPI,
    Plain,
    ScramSha256,
    ScramSha512,
    OauthBearer
}

impl fmt::Display for SaslMechanism {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let a = match self {
            Self::GssAPI => "GSSAPI",
            Self::Plain => "PLAIN",
            Self::ScramSha256 => "SCRAM-SHA-256",
            Self::ScramSha512 => "SCRAM-SHA-512",
            Self::OauthBearer => "OAUTHBEARER"
        };
        write!(f, "{}", a)
    }
}


#[derive(Debug, Copy, Clone)]
pub enum EndpointIdentificationAlgorithm {
    None,
    Https
}

impl fmt::Display for EndpointIdentificationAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let a = match self {
            Self::None => "none",
            Self::Https => "https",
        };
        write!(f, "{}", a)
    }
}
