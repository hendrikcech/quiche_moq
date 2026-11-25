use clap::Parser;

fn main() {
    env_logger::builder().filter_level(log::LevelFilter::Info).parse_default_env().init();
    if let Err(error) = moq_cli::node::run(moq_cli::Args::parse()) {
        log::error!("{error}");
        std::process::exit(1);
    }
}
