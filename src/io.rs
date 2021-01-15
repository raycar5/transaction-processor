use futures::{StreamExt, TryStream};
use parse_display::{Display, FromStr};
use serde::Serialize;
use smol::io::AsyncBufReadExt;
use std::{
    io::{Error, ErrorKind},
    str::FromStr,
};

#[derive(Serialize, Display, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// Uniquely identifies a Deposit or Withdraw transaction.
pub struct TransactionId(pub u32);
#[derive(Serialize, Display, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// Uniquely identifies a Client.
pub struct ClientId(pub u16);

#[derive(Display, FromStr, Clone, Copy, PartialEq, Debug)]
#[display(style = "lowercase")]
/// Represents all the different types of transactions.
///
/// Used for parsing and serializing.
enum TransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    ChargeBack,
}

/// Represents an input transaction line in the input csv.
#[derive(Debug, PartialEq)]
pub enum Transaction {
    Deposit {
        client: ClientId,
        tx: TransactionId,
        amount: f64,
    },
    Withdrawal {
        client: ClientId,
        tx: TransactionId,
        amount: f64,
    },
    Dispute {
        client: ClientId,
        tx: TransactionId,
    },
    Resolve {
        client: ClientId,
        tx: TransactionId,
    },
    ChargeBack {
        client: ClientId,
        tx: TransactionId,
    },
}

impl Transaction {
    /// Returns the header for serializing transactions to csv.
    pub fn header() -> &'static str {
        "type, client, tx, amount"
    }

    /// Returns a csv line representing this transaction.
    pub fn to_csv(&self) -> String {
        let (ty, client, tx, amount) = match self {
            Transaction::Deposit { client, tx, amount } => {
                (TransactionType::Deposit, client, tx, Some(amount))
            }
            Transaction::Withdrawal { client, tx, amount } => {
                (TransactionType::Withdrawal, client, tx, Some(amount))
            }
            Transaction::Dispute { client, tx } => (TransactionType::Dispute, client, tx, None),
            Transaction::Resolve { client, tx } => (TransactionType::Resolve, client, tx, None),
            Transaction::ChargeBack { client, tx } => {
                (TransactionType::ChargeBack, client, tx, None)
            }
        };
        let amount = amount
            .map(|f| f.to_string())
            .unwrap_or_else(|| "".to_string());
        format!("{},{},{},{}", ty, client, tx, amount)
    }

    /// Returns the [ClientId] of this transaction.
    pub fn client(&self) -> ClientId {
        use Transaction::*;
        match self {
            Deposit { client, .. }
            | Withdrawal { client, .. }
            | Dispute { client, .. }
            | Resolve { client, .. }
            | ChargeBack { client, .. } => *client,
        }
    }
}

#[derive(Serialize, Debug)]
/// Represents an output account line in the output csv.
pub struct Output {
    pub client: ClientId,
    pub available: f64,
    pub held: f64,
    pub total: f64,
    pub locked: bool,
}

/// Returns a [TryStream] of [Transaction] from a byte buffer that contains a csv file.
///
/// The csv columns must follow the order dictated by [Transaction::header].
/// Whitespace is ignored in the rows.
pub fn parse(
    read: impl AsyncBufReadExt + Unpin,
) -> impl TryStream<Ok = Transaction, Error = Error> {
    let lines = read.lines().enumerate().skip(1);

    lines.map(move |(i, line)| {
        line.and_then(|line| {
            let mut elems = line.split(',').take(4).map(|e| e.trim());

            let ty = elems
                .next()
                .and_then(|e| FromStr::from_str(e).ok())
                .ok_or_else(field_error("type", i))?;

            let client = elems
                .next()
                .and_then(|e| atoi::atoi(e.as_bytes()))
                .map(ClientId)
                .ok_or_else(field_error("client", i))?;

            let tx = elems
                .next()
                .and_then(|e| atoi::atoi(e.as_bytes()))
                .map(TransactionId)
                .ok_or_else(field_error("tx", i))?;

            let mut get_amount = move || {
                elems
                    .next()
                    .and_then(|e| fast_float::parse::<f64, _>(e).ok())
                    .ok_or_else(field_error("amount", i))
            };

            let transaction = match ty {
                TransactionType::Deposit => Transaction::Deposit {
                    client,
                    tx,
                    amount: get_amount()?,
                },
                TransactionType::Withdrawal => Transaction::Withdrawal {
                    client,
                    tx,
                    amount: get_amount()?,
                },
                TransactionType::Dispute => Transaction::Dispute { client, tx },
                TransactionType::Resolve => Transaction::Resolve { client, tx },
                TransactionType::ChargeBack => Transaction::ChargeBack { client, tx },
            };
            Ok(transaction)
        })
    })
}

fn field_error(field: &'static str, i: usize) -> impl Fn() -> Error {
    move || {
        Error::new(
            ErrorKind::InvalidData,
            format!("Missing or invalid {} in line {}", field, i),
        )
    }
}

#[cfg(test)]
mod tests {
    use futures::io::BufReader;
    use futures::TryStreamExt;
    use rand::Rng;

    use super::*;

    #[smol_potat::test]
    async fn invalid_type() {
        // Invalid string
        let br = BufReader::new(
            r#"
        test
        "#
            .as_bytes(),
        );
        assert_eq!(
            parse(br).try_next().await.unwrap_err().to_string(),
            field_error("type", 1)().to_string()
        );

        // Number
        let br = BufReader::new(
            r#"
        1
        "#
            .as_bytes(),
        );
        assert_eq!(
            parse(br).try_next().await.unwrap_err().to_string(),
            field_error("type", 1)().to_string()
        );

        // Empty
        let br = BufReader::new(
            r#"
        ,1
        "#
            .as_bytes(),
        );
        assert_eq!(
            parse(br).try_next().await.unwrap_err().to_string(),
            field_error("type", 1)().to_string()
        );
    }

    #[smol_potat::test]
    async fn invalid_client() {
        // String
        let br = BufReader::new(
            r#"
        deposit, foo
        "#
            .as_bytes(),
        );
        assert_eq!(
            parse(br).try_next().await.unwrap_err().to_string(),
            field_error("client", 1)().to_string()
        );

        // Negative number
        let br = BufReader::new(
            r#"
        deposit, -3
        "#
            .as_bytes(),
        );
        assert_eq!(
            parse(br).try_next().await.unwrap_err().to_string(),
            field_error("client", 1)().to_string()
        );

        // Overflow
        let string = format!(
            r#"
        deposit, {}
        "#,
            std::u16::MAX as u32 + 1
        );
        let br = BufReader::new(string.as_bytes());
        assert_eq!(
            parse(br).try_next().await.unwrap_err().to_string(),
            field_error("client", 1)().to_string()
        );

        // Empty
        let br = BufReader::new(
            r#"
        deposit, ,4
        "#
            .as_bytes(),
        );
        assert_eq!(
            parse(br).try_next().await.unwrap_err().to_string(),
            field_error("client", 1)().to_string()
        );
    }

    #[smol_potat::test]
    async fn invalid_tx() {
        // String
        let br = BufReader::new(
            r#"
        deposit, 4, bar
        "#
            .as_bytes(),
        );
        assert_eq!(
            parse(br).try_next().await.unwrap_err().to_string(),
            field_error("tx", 1)().to_string()
        );

        // Negative number
        let br = BufReader::new(
            r#"
        deposit,5, -5
        "#
            .as_bytes(),
        );
        assert_eq!(
            parse(br).try_next().await.unwrap_err().to_string(),
            field_error("tx", 1)().to_string()
        );

        // Overflow
        let string = format!(
            r#"
        deposit,6, {}
        "#,
            std::u32::MAX as u64 + 1
        );
        let br = BufReader::new(string.as_bytes());
        assert_eq!(
            parse(br).try_next().await.unwrap_err().to_string(),
            field_error("tx", 1)().to_string()
        );

        // Empty
        let br = BufReader::new(
            r#"
        deposit, 4,,
        "#
            .as_bytes(),
        );
        assert_eq!(
            parse(br).try_next().await.unwrap_err().to_string(),
            field_error("tx", 1)().to_string()
        );
    }
    #[smol_potat::test]
    async fn invalid_amount() {
        // String
        let br = BufReader::new(
            r#"
        deposit, 2, 3 , eheh 
        "#
            .as_bytes(),
        );
        assert_eq!(
            parse(br).try_next().await.unwrap_err().to_string(),
            field_error("amount", 1)().to_string()
        );

        // Empty
        let br = BufReader::new(
            r#"
        deposit, 3,4,5
        deposit, 2, 3 , 
        "#
            .as_bytes(),
        );
        let mut txs = parse(br);
        txs.try_next().await.unwrap();
        assert_eq!(
            txs.try_next().await.unwrap_err().to_string(),
            field_error("amount", 2)().to_string()
        );
    }
    #[smol_potat::test]
    async fn test_parse() {
        let br = BufReader::new(
            r#"
        deposit, 1.6 , 3.3 , 5.7  
        withdrawal,2,5,9 
              dispute    ,   8       ,    4   
        resolve, 9, 30,
        chargeback, 24, 2000   
        "#
            .as_bytes(),
        );
        let mut txs = parse(br);
        assert_eq!(
            txs.try_next().await.unwrap().unwrap(),
            Transaction::Deposit {
                client: ClientId(1),
                tx: TransactionId(3),
                amount: 5.7
            }
        );

        assert_eq!(
            txs.try_next().await.unwrap().unwrap(),
            Transaction::Withdrawal {
                client: ClientId(2),
                tx: TransactionId(5),
                amount: 9.
            }
        );
        assert_eq!(
            txs.try_next().await.unwrap().unwrap(),
            Transaction::Dispute {
                client: ClientId(8),
                tx: TransactionId(4),
            }
        );
        assert_eq!(
            txs.try_next().await.unwrap().unwrap(),
            Transaction::Resolve {
                client: ClientId(9),
                tx: TransactionId(30),
            }
        );
        assert_eq!(
            txs.try_next().await.unwrap().unwrap(),
            Transaction::ChargeBack {
                client: ClientId(24),
                tx: TransactionId(2000),
            }
        );
    }

    #[smol_potat::test]
    async fn test_no_panic() {
        let mut rng = rand::thread_rng();
        let mut bytes = [0u8; 1024];
        for _ in 0..1000usize {
            rng.fill(&mut bytes);
            let mut txs = parse(BufReader::new(&bytes[..]));
            while let Ok(Some(i)) = txs.try_next().await {
                bencher::black_box(i);
            }
        }
    }

    #[test]
    fn transaction_to_csv() {
        assert_eq!(
            "deposit,1,1,3.4",
            Transaction::Deposit {
                client: ClientId(1),
                tx: TransactionId(1),
                amount: 3.4
            }
            .to_csv()
        );

        assert_eq!(
            "withdrawal,5,10,34",
            Transaction::Withdrawal {
                client: ClientId(5),
                tx: TransactionId(10),
                amount: 34.
            }
            .to_csv()
        );

        assert_eq!(
            "dispute,59,999,",
            Transaction::Dispute {
                client: ClientId(59),
                tx: TransactionId(999),
            }
            .to_csv()
        );

        assert_eq!(
            "resolve,89,7,",
            Transaction::Resolve {
                client: ClientId(89),
                tx: TransactionId(7),
            }
            .to_csv()
        );

        assert_eq!(
            "chargeback,34040,33304304,",
            Transaction::ChargeBack {
                client: ClientId(34040),
                tx: TransactionId(33304304),
            }
            .to_csv()
        );
    }
}
