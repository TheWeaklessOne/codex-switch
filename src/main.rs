use std::process::ExitCode;

fn main() -> ExitCode {
    match codex_switch::cli::run(std::env::args_os()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(codex_switch::error::AppError::JsonFailureRendered) => ExitCode::from(2),
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::from(2)
        }
    }
}
