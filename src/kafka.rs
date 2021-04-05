use std::future::Future;
use futures::future::{FutureExt};
use futures_timer::Delay;
use std::time::Duration;
use rdkafka::util::AsyncRuntime;

pub struct BastionRuntime;

impl AsyncRuntime for BastionRuntime {
    type Delay = futures::future::Map<futures_timer::Delay, fn(())>;

    fn spawn<T>(task: T)
        where
            T: Future<Output = ()> + Send + 'static,
    {
        let _ = bastion::io::spawn(task);
    }

    fn delay_for(duration: Duration) -> Self::Delay {
        FutureExt::map(Delay::new(duration), |_| ())
    }
}

pub struct CTopic {}