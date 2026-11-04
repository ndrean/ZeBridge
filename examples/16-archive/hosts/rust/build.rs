fn main() {
    let dir = std::env::var("ZBARCHIVE_LIB_DIR").unwrap_or_else(|_| "../../../../zig-out/lib".into());
    println!("cargo:rustc-link-search=native={dir}");
    println!("cargo:rerun-if-env-changed=ZBARCHIVE_LIB_DIR");
}
