fn main() {
    if let Err(error) = xtask::run(std::env::args().skip(1).collect()) {
        eprintln!("xtask: {error}");
        std::process::exit(1);
    }
}
