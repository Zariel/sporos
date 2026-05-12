use std::process::ExitCode;

fn main() -> ExitCode {
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
