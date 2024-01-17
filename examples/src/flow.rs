use callysto::prelude::*;
use callysto::futures::stream;
use futures::stream::{Stream, StreamExt};
use callysto::futures::*;
use callysto::errors::{Result as CResult, *};
use std::ops::Mul;


async fn multiplier<S>(mut s: CSource<S>, ctx: Context<()>) -> CResult<()>
where
    S: Stream + Clone + Send + Sync + Unpin,
    <S as Stream>::Item: std::fmt::Debug
{
    while let Some(x) = s.next().await {
        println!("{:?}", x);
    }
    Ok::<(), CallystoError>(())
}

fn main() {
    let mut app = Callysto::new();
    app.with_name("flow");

    let a = [1, 2, 3, 4, 5];
    let source = app.source(stream::iter(a));
    app.flow("multiplier", source, multiplier);

    app.run();
}
