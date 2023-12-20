use callysto::futures::AsyncReadExt;
use callysto::http_types::StatusCode;
use callysto::prelude::*;
use nuclei::Handle;
use std::fs::File;

async fn cpuinfo(_req: CWebRequest, _ctx: Context<()>) -> CWebResult<CWebResponse> {
    let mut res = CWebResponse::new(StatusCode::Ok);
    let fo = File::open("/proc/cpuinfo").unwrap();
    let mut file = Handle::<File>::new(fo).unwrap();
    let mut cpuinfo = String::new();
    file.read_to_string(&mut cpuinfo).await;
    res.insert_header("Content-Type", "text/plain");
    res.set_body(cpuinfo);
    Ok(res)
}

async fn meminfo(_req: CWebRequest, _ctx: Context<()>) -> CWebResult<CWebResponse> {
    let mut res = CWebResponse::new(StatusCode::Ok);
    let fo = File::open("/proc/meminfo").unwrap();
    let mut file = Handle::<File>::new(fo).unwrap();
    let mut meminfo = String::new();
    file.read_to_string(&mut meminfo).await;
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
