#![allow(unused_imports)]
#![allow(dead_code)]
#![allow(unused_must_use)]
#![allow(unused)]
#![allow(incomplete_features)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/vertexclique/callysto/master/art/callysto_logo.png"
)]

pub mod avro;

pub mod prelude {
    pub use super::avro::*;
    pub use apache_avro::*;
}
