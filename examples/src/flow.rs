use callysto::prelude::*;
use callysto::futures::stream;
use futures::stream::{self, StreamExt};
use callysto::futures::*;
use std::ops::Mul;


async fn multiplier<S: Stream + std::marker::Unpin>(s: &S, ctx: Context<()>) -> Result<()>
where
    <S as futures::Stream>::Item: Mul,
    <<S as futures::Stream>::Item as Mul>::Output: std::fmt::Display
{
    while let Some(x) = s.next().await {
        println!("{}", x*x);
    }
    Ok(())
}

fn main() {
    let mut app = Callysto::new();
    app.with_name("flow");

    let a = [1, 2, 3, 4, 5];
    let stream = stream::iter(a.iter()).cycle();
    app.flow("multiplier", stream, multiplier);

    app.run();
}
