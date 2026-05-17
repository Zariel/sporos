use std::process::ExitCode;

fn main() -> ExitCode {
    if let Err(error) = sporos::logging::init_from_env() {
        eprintln!("sporos: failed to initialize logging: {error}");
        return ExitCode::FAILURE;
    }

    match sporos::cli::run(std::env::args_os()) {
        Ok(output) => {
            if !output.is_empty() {
                println!("{output}");
            }
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("sporos: {error}");
            ExitCode::FAILURE
        }
    }
}
