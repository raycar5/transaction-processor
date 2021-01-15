use rustc_hash::FxHashMap;
use std::iter::Map;

#[cfg(feature = "multithreaded")]
use futures::StreamExt;
use smol::io::BufReader;
#[cfg(feature = "multithreaded")]
use smol::{
    channel::{bounded, Receiver},
    spawn,
};

use crate::{
    client_state::ClientState,
    io::{parse, ClientId, Output, Transaction},
};
use futures::stream::TryStreamExt;
use smol::{fs::File, Unblock};
use std::error::Error;

/// Manages the state of a group of clients.
///
/// Can be used single threaded by simply calling [TransactionProcessor::process]
/// or multi threaded by providing a [Receiver] and calling [TransactionProcessor::run].
struct TransactionProcessor {
    clients: FxHashMap<ClientId, ClientState>,
    #[cfg(feature = "multithreaded")]
    rx: Receiver<Transaction>,
}
impl TransactionProcessor {
    #[cfg(feature = "multithreaded")]
    /// Returns an empty multi threaded [TransactionProcessor].
    pub fn new(rx: Receiver<Transaction>) -> TransactionProcessor {
        TransactionProcessor {
            clients: Default::default(),
            rx,
        }
    }
    /// Returns an empty single threaded [TransactionProcessor].
    #[cfg(not(feature = "multithreaded"))]
    pub fn new() -> TransactionProcessor {
        TransactionProcessor {
            clients: Default::default(),
        }
    }
    #[cfg(feature = "multithreaded")]
    /// Processes all transactions received through `rx` until `rx` closes.
    pub async fn run(&mut self) {
        while let Some(tx) = self.rx.next().await {
            self.process(tx)
        }
    }
    /// Forwards `tx` to the appropriate client for processing.
    pub fn process(&mut self, tx: Transaction) {
        self.clients
            .entry(tx.client())
            .or_default()
            .process_transaction(tx)
    }
}

type IntoIter = Map<
    <FxHashMap<ClientId, ClientState> as IntoIterator>::IntoIter,
    fn((ClientId, ClientState)) -> Output,
>;
impl IntoIterator for TransactionProcessor {
    type IntoIter = IntoIter;
    type Item = Output;
    fn into_iter(self) -> Self::IntoIter {
        self.clients.into_iter().map(Into::into)
    }
}

// Tuned with time_multi.sh
#[cfg(feature = "multithreaded")]
/// Number of messages in the channels between tasks.
const MESSAGE_BUFFER: usize = 100000;

/// Processes the transactions in `file_in` and outputs the resulting [Outputs](Output) to stdout.
pub async fn process(file_in: &str) -> Result<(), Box<dyn Error>> {
    // Create a transaction stream.
    let file = File::open(file_in).await?;
    // Bigger buffer shaves a few milliseconds.
    let mut transactions = parse(BufReader::with_capacity(100 * 1024, file));

    // Create an output writer.
    let mut wri = csv_async::AsyncSerializer::from_writer(Unblock::new(std::io::stdout()));

    #[cfg(not(feature = "multithreaded"))]
    {
        // Process each transaction.
        let mut tp = TransactionProcessor::new();
        while let Some(transaction) = transactions.try_next().await? {
            tp.process(transaction)
        }

        // Output to stdout.
        for output in tp {
            wri.serialize(output).await?
        }
    }

    // Faster on bigger datasets that have few chargebacks.
    // If there are many chargebacks the processing step basically becomes only a hashmap lookup.
    #[cfg(feature = "multithreaded")]
    {
        //let cpus = num_cpus::get() - 2;
        // Having only one extra cpu is better than all available threads.
        // Makes sense, one thread parses and the other processes, once you get more it's not worth the channel overhead.
        // Flamegraph corroborates this.
        let cpus = 1;

        let mut txs = Vec::new();
        let mut tasks = Vec::new();

        txs.reserve(cpus);
        tasks.reserve(cpus);

        // Create a processor per cpu and a channel to send transactions to it.
        for _ in 0..cpus {
            let (tx, rx) = bounded::<Transaction>(MESSAGE_BUFFER);

            txs.push(tx);

            tasks.push(spawn(async move {
                let mut tp = TransactionProcessor::new(rx);
                tp.run().await;
                // Once finished, the processor will return an iter of outputs.
                tp.into_iter()
            }))
        }

        while let Some(transaction) = transactions.try_next().await? {
            // Transactions are partitioned by client id, assuming there is a uniform
            // distribution of client ids, this should be very efficient.
            txs[transaction.client().0 as usize % cpus]
                .send(transaction)
                .await?;
        }

        // Close all channels to signal the processors that we are done.
        for tx in txs {
            tx.close();
        }

        // For each processor output the Output iter to stdout.
        // This could be more efficient with a way to select! on a slice but
        // I didn't find any good implementation and the output phase is not
        // even visible in the flamegraph so it's probably not necessary.
        for task in tasks {
            for output in task.await {
                wri.serialize(output).await?
            }
        }
    }

    Ok(())
}
