use callysto::prelude::*;

fn main() {
    let mut app = Callysto::new();
    app.with_name("web-app");

    app.run();
}
