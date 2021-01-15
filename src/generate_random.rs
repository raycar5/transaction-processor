use rand::Rng;

use futures::AsyncWriteExt;
use smol::{fs::File, io::BufWriter};
use std::error::Error;

use crate::io::{ClientId, Transaction, TransactionId};

const LINES: usize = 10000000;

pub async fn generate_random(file_out: &str) -> Result<(), Box<dyn Error>> {
    let mut wri = BufWriter::new(File::create(file_out).await?);
    wri.write(Transaction::header().as_bytes()).await?;
    wri.write(b"\n").await?;

    let mut rng = rand::thread_rng();

    for _ in 0..LINES {
        let transaction = match rng.gen_range(0..5) {
            0 => Transaction::Deposit {
                client: ClientId(rng.gen()),
                tx: TransactionId(rng.gen()),
                amount: rng.gen(),
            },
            1 => Transaction::Withdrawal {
                client: ClientId(rng.gen()),
                tx: TransactionId(rng.gen()),
                amount: rng.gen(),
            },
            2 => Transaction::Dispute {
                client: ClientId(rng.gen()),
                tx: TransactionId(rng.gen()),
            },
            3 => Transaction::Resolve {
                client: ClientId(rng.gen()),
                tx: TransactionId(rng.gen()),
            },
            4 => Transaction::ChargeBack {
                client: ClientId(rng.gen()),
                tx: TransactionId(rng.gen()),
            },
            _ => unreachable!(),
        };
        wri.write(transaction.to_csv().as_bytes()).await?;
        wri.write(b"\n").await?;
    }

    Ok(())
}
