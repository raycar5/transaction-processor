use futures::AsyncWriteExt;
use rand::Rng;
use smol::{fs::File, io::BufWriter};
use std::error::Error;

use crate::io::{ClientId, Transaction, TransactionId};

const LINES: usize = 10000000;

pub async fn generate(file_out: &str) -> Result<(), Box<dyn Error>> {
    let mut wri = BufWriter::new(File::create(file_out).await?);
    wri.write(Transaction::header().as_bytes()).await?;
    wri.write(b"\n").await?;

    let mut rng = rand::thread_rng();
    let mut tx_id = 0u32;
    let mut client_id = 1u16;
    let mut deposits = Vec::new();
    let mut clients = vec![ClientId(0u16)];

    for _ in 0..LINES {
        let transaction = match rng.gen_range(0..100) {
            0..=25 => {
                let client = if rng.gen_bool(0.2) {
                    let client = ClientId(client_id);

                    clients.push(client);
                    client_id += 1;
                    client
                } else {
                    clients[rng.gen_range(0..clients.len())]
                };

                let tx = TransactionId(tx_id);
                tx_id += 1;
                deposits.push((tx, client));

                Transaction::Deposit {
                    client,
                    tx,
                    amount: rng.gen_range(0.0..1000.0),
                }
            }
            26..=50 => {
                let tx = TransactionId(tx_id);
                tx_id += 1;

                let client = clients[rng.gen_range(0..clients.len())];

                Transaction::Withdrawal {
                    client,
                    tx,
                    amount: rng.gen_range(0.0..1000.0),
                }
            }
            51..=70 => {
                if deposits.is_empty() {
                    continue;
                }
                let (tx, client) = deposits[rng.gen_range(0..deposits.len())];

                Transaction::Dispute { client, tx }
            }
            71..=98 => {
                if deposits.is_empty() {
                    continue;
                }
                let (tx, client) = deposits[rng.gen_range(0..deposits.len())];

                Transaction::Resolve { client, tx }
            }
            // Low probability because with enough transactions, most users were ending up in the locked state.
            // which makes sense.
            99..=100 => {
                if deposits.is_empty() {
                    continue;
                }
                let (tx, client) = deposits[rng.gen_range(0..deposits.len())];

                Transaction::ChargeBack { client, tx }
            }
            _ => {
                unreachable!()
            }
        };
        wri.write(transaction.to_csv().as_bytes()).await?;
        wri.write(b"\n").await?;
    }

    Ok(())
}
