use callysto::http_types::StatusCode;
use callysto::prelude::*;

async fn cpuinfo(_req: CWebRequest, _ctx: Context<()>) -> CWebResult<CWebResponse> {
    let mut res = CWebResponse::new(StatusCode::Ok);
    let cpuinfo = std::fs::read_to_string("/proc/cpuinfo").unwrap();
    res.insert_header("Content-Type", "text/plain");
    res.set_body(cpuinfo);
    Ok(res)
}

async fn meminfo(_req: CWebRequest, _ctx: Context<()>) -> CWebResult<CWebResponse> {
    let mut res = CWebResponse::new(StatusCode::Ok);
    let meminfo = std::fs::read_to_string("/proc/meminfo").unwrap();
    res.insert_header("Content-Type", "text/plain");
    res.set_body(meminfo);
    Ok(res)
}

fn main() {
    let mut app = Callysto::new();
    app.with_name("web-app");

    app.page("/cpuinfo", cpuinfo);
    app.page("/meminfo", meminfo);

    app.run();
}
