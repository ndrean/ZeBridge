//! The native archiver from Rust, through libzbarchive (include/zbarchive.h).
//!
//!   ZBARCHIVE_LIB_DIR=$PWD/zig-out/lib cargo build --manifest-path examples/16-archive/hosts/rust/Cargo.toml
//!   LD_LIBRARY_PATH=zig-out/lib examples/16-archive/hosts/rust/target/debug/zba-rust '{"tenant":"acme"}'
//!
//! Exits with the archiver's code; a Debug build of the library that leaked exits 70 instead.
//! A host that wants to stop it calls `zba_stop` from any thread (a signal handler included).

use std::ffi::{c_char, c_int, c_void, CStr, CString};

#[repr(C)]
struct Zba {
    _private: [u8; 0],
}

type LogFn = extern "C" fn(ctx: *mut c_void, level: u8, line: *const c_char);

#[link(name = "zbarchive")]
extern "C" {
    fn zba_abi_version() -> c_int;
    fn zba_open(config_json: *const c_char, log: Option<LogFn>, ctx: *mut c_void) -> *mut Zba;
    fn zba_last_error() -> *const c_char;
    fn zba_run(a: *mut Zba) -> c_int;
    fn zba_close(a: *mut Zba);
    fn zba_leaks() -> u32;
}

extern "C" fn on_line(_ctx: *mut c_void, level: u8, line: *const c_char) {
    let text = unsafe { CStr::from_ptr(line) }.to_string_lossy();
    if level > 0 {
        eprintln!("[rust host] {text}");
    } else {
        println!("[rust host] {text}");
    }
}

fn main() {
    let cfg = std::env::args().nth(1).unwrap_or_else(|| {
        eprintln!("usage: zba-rust '<config json>'");
        std::process::exit(1)
    });
    if unsafe { zba_abi_version() } != 1 {
        eprintln!("libzbarchive ABI {}, this host knows 1", unsafe { zba_abi_version() });
        std::process::exit(1);
    }
    let cfg = CString::new(cfg).expect("no NUL in the configuration");
    let a = unsafe { zba_open(cfg.as_ptr(), Some(on_line), std::ptr::null_mut()) };
    if a.is_null() {
        eprintln!("{}", unsafe { CStr::from_ptr(zba_last_error()) }.to_string_lossy());
        std::process::exit(1);
    }
    let code = unsafe { zba_run(a) };
    unsafe { zba_close(a) };
    if unsafe { zba_leaks() } != 0 {
        std::process::exit(70);
    }
    std::process::exit(code);
}
