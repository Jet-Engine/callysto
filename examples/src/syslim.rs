use std::convert::TryFrom;
use std::ffi::CString;
use std::path::Path;

fn main() {
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGE_SIZE) };
    let page_count = unsafe { libc::sysconf(libc::_SC_PHYS_PAGES) };

    // fd max per proc
    let mut limit = std::mem::MaybeUninit::<libc::rlimit>::uninit();
    let ret = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, limit.as_mut_ptr()) };
    if ret != 0 {
        panic!("RLIMIT_NOFILE Err: {}", std::io::Error::last_os_error());
    }
    let limit = unsafe { limit.assume_init() };

    // disk
    let path: CString = Path::new("/")
        .to_str()
        .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidInput))
        .and_then(|s| {
            CString::new(s).map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))
        })
        .unwrap();

    let mut vfs = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    let ret = unsafe { libc::statvfs(path.as_ptr(), vfs.as_mut_ptr()) };
    if ret != 0 {
        panic!("FS STAT Err: {}", std::io::Error::last_os_error());
    }

    let vfs = unsafe { vfs.assume_init() };

    println!(
        "page_size: {}, page_count: {}, maxopenfile: {}, free_disk: {}",
        page_size,
        page_count,
        usize::try_from(limit.rlim_max).unwrap(),
        u64::try_from(vfs.f_bavail as u64 * vfs.f_frsize).unwrap()
    );
}
