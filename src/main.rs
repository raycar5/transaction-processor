use std::process;
mod client_state;
mod generate;
mod generate_random;
mod io;
mod transaction_processor;
use generate::generate;
use generate_random::generate_random;
use transaction_processor::process;

/// Arguments: `<file>` `<operation>`
///
/// `<file>`
/// Path to the file to operate on.
///
/// `<operation>`
/// Can be "", "gen" or "genrandom"
/// "" -> Processes the transactions in `<file>` and outputs the result to stdout.
/// "gen" -> Generates transactions using a smart-ish algorithm and outputs them to `<file>`.
/// "genrandom" -> Generates transactions using purely random values and outputs them to `<file>`.
async fn async_main() {
    let mut args = std::env::args().into_iter().skip(1);
    let file = args
        .next()
        .expect("Please provide a path to a csv file in the first argument");

    let res = match args.next().as_deref() {
        None => process(&file).await,
        Some("gen") => generate(&file).await,
        Some("genrandom") => generate_random(&file).await,
        _ => panic!("The second argument can only be 'gen' or 'genrandom'"),
    };

    if let Err(err) = res {
        eprintln!("error processing transactions: {}", err);
        process::exit(1);
    }
}

#[cfg(feature = "multithreaded")]
#[smol_potat::main(threads = 2)]
async fn main() {
    async_main().await
}

#[cfg(not(feature = "multithreaded"))]
#[smol_potat::main]
async fn main() {
    async_main().await
}
