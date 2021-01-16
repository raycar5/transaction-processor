use rustc_hash::FxHashMap;

use crate::io::{ClientId, Output, Transaction, TransactionId};

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
/// Represents the different states that a deposit can be in.
enum DepositStateType {
    Normal,
    Disputed,
    ChargedBack,
}
#[derive(PartialEq, Debug, Clone, Copy)]
/// Aggregates the information of a single deposit.
struct DepositState {
    ty: DepositStateType,
    amount: f64,
}
impl DepositState {
    /// Returns a new DepositState with [DepositStateType::Normal]
    /// and the amount provided.
    fn new(amount: f64) -> Self {
        Self {
            ty: DepositStateType::Normal,
            amount,
        }
    }
}
#[derive(PartialEq, Debug, Clone)]
/// Aggregates the information of a single client.
pub struct ClientState {
    deposits: FxHashMap<TransactionId, DepositState>,
    available: f64,
    held: f64,
    locked: bool,
}
impl Default for ClientState {
    fn default() -> Self {
        Self {
            deposits: Default::default(),
            available: 0.0,
            held: 0.0,
            locked: false,
        }
    }
}

impl ClientState {
    /// Updates the [ClientState] based on a new [Transaction]
    ///
    /// Refer to the assignment doc for the precise semantics of this function.
    /// I would put a link here but I don't have one.
    pub fn process_transaction(&mut self, tx: Transaction) {
        if self.locked {
            return;
        }

        use Transaction::*;
        // I have consciously made the choice to leave the logic all in the same match statement for conciseness and maintainability.
        // If the match arms got out of hand or contained a lot of complex logic, they should be moved to their own functions.
        match tx {
            Deposit { tx, amount, .. } => {
                self.deposits.insert(tx, DepositState::new(amount));
                self.available += amount
            }
            Withdrawal { client, amount, .. } => {
                if self.available - amount < 0.0 {
                    handle_insufficient_funds(client, amount, self.available);
                    return;
                }
                self.available -= amount
            }
            Dispute { client, tx } => {
                if let Some(deposit) = self.deposits.get_mut(&tx) {
                    if deposit.ty != DepositStateType::Normal {
                        handle_already_disputed_deposit(client, tx);
                        return;
                    }
                    deposit.ty = DepositStateType::Disputed;
                    self.available -= deposit.amount;
                    self.held += deposit.amount;
                } else {
                    handle_non_existent_deposit(client, tx);
                }
            }
            Resolve { client, tx } => {
                if let Some(deposit) = self.deposits.get_mut(&tx) {
                    if deposit.ty != DepositStateType::Disputed {
                        handle_not_disputed_deposit(client, tx);
                        return;
                    }
                    deposit.ty = DepositStateType::Normal;
                    self.available += deposit.amount;
                    self.held -= deposit.amount;
                } else {
                    handle_non_existent_deposit(client, tx);
                }
            }
            ChargeBack { client, tx } => {
                if let Some(deposit) = self.deposits.get_mut(&tx) {
                    if deposit.ty != DepositStateType::Disputed {
                        handle_not_disputed_deposit(client, tx);
                        return;
                    }
                    deposit.ty = DepositStateType::ChargedBack;
                    self.held -= deposit.amount;
                    self.locked = true;
                    handle_account_locked(client, tx);
                } else {
                    handle_non_existent_deposit(client, tx);
                }
            }
        }
    }
}

// In a production system, these functions would submit anonymized structured logs and probably a notification to some security system.
#[allow(unused_variables)]
fn handle_insufficient_funds(client: ClientId, amount: f64, available: f64) {
    #[cfg(feature = "stderr")]
    eprintln!(
        "Client: {} attempted to withdraw {} while only {} were available in his account.",
        client, amount, available
    );
}
#[allow(unused_variables)]
fn handle_already_disputed_deposit(client: ClientId, tx: TransactionId) {
    #[cfg(feature = "stderr")]
    eprintln!(
        "Client: {} attempted to dispute transaction {} which had already been disputed.",
        client, tx
    );
}
#[allow(unused_variables)]
fn handle_not_disputed_deposit(client: ClientId, tx: TransactionId) {
    #[cfg(feature = "stderr")]
    eprintln!(
        "Client: {} attempted to resolve or charge back transaction {} which is not disputed.",
        client, tx
    );
}
#[allow(unused_variables)]
fn handle_non_existent_deposit(client: ClientId, tx: TransactionId) {
    #[cfg(feature = "stderr")]
    eprintln!(
        "Client: {} attempted to dispute a non existent deposit {}.",
        client, tx
    );
}
// In a production system this would probably send a notification to other services which would
// contact the user and the customer support team.
#[allow(unused_variables)]
fn handle_account_locked(client: ClientId, tx: TransactionId) {
    #[cfg(feature = "stderr")]
    eprintln!(
        "Client: {} is locked after issuing a chargeback for deposit: {}.",
        client, tx
    );
}

impl Into<Output> for (ClientId, ClientState) {
    fn into(self) -> Output {
        let (
            client,
            ClientState {
                available,
                held,
                locked,
                ..
            },
        ) = self;
        Output {
            client,
            available,
            held,
            total: available + held,
            locked,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! maplit {
        ($($k:expr=>$v:expr),*) => {
            [$(($k,$v)),*].iter().copied().collect()
        };
    }
    // In a production system, I would test i/o caused by handling error conditions as well.
    #[test]
    fn test_deposit() {
        let mut cs = ClientState::default();

        cs.process_transaction(Transaction::Deposit {
            client: ClientId(1),
            tx: TransactionId(1),
            amount: 3.,
        });
        assert_eq!(
            cs,
            ClientState {
                available: 3.,
                held: 0.,
                locked: false,
                deposits: maplit! {
                    TransactionId(1) =>
                    DepositState {
                        amount: 3.0,
                        ty: DepositStateType::Normal
                    }
                }
            }
        );

        cs.process_transaction(Transaction::Deposit {
            client: ClientId(1),
            tx: TransactionId(2),
            amount: 5.,
        });
        assert_eq!(
            cs,
            ClientState {
                available: 8.,
                held: 0.,
                locked: false,
                deposits: maplit! {
                    TransactionId(1) =>
                    DepositState {
                        amount: 3.0,
                        ty: DepositStateType::Normal
                    },
                    TransactionId(2) =>
                    DepositState {
                        amount: 5.0,
                        ty: DepositStateType::Normal
                    }
                }
            }
        );
    }
    #[test]
    fn test_withdraw() {
        let mut cs = ClientState::default();

        // Withdraw empty account.
        let old_cs = cs.clone();
        cs.process_transaction(Transaction::Withdrawal {
            client: ClientId(1),
            tx: TransactionId(1),
            amount: 2.,
        });
        assert_eq!(cs, old_cs);

        // Successful withdrawal.
        cs.process_transaction(Transaction::Deposit {
            client: ClientId(1),
            tx: TransactionId(2),
            amount: 3.,
        });
        cs.process_transaction(Transaction::Withdrawal {
            client: ClientId(1),
            tx: TransactionId(3),
            amount: 2.,
        });
        assert_eq!(
            cs,
            ClientState {
                available: 1.,
                held: 0.,
                locked: false,
                deposits: maplit! {
                    TransactionId(2) =>
                    DepositState {
                        amount: 3.0,
                        ty: DepositStateType::Normal
                    }
                }
            }
        );

        // Too little funds.
        let old_cs = cs.clone();
        cs.process_transaction(Transaction::Withdrawal {
            client: ClientId(1),
            tx: TransactionId(2),
            amount: 2.,
        });
        assert_eq!(cs, old_cs);
    }

    #[test]
    fn test_dispute() {
        let mut cs = ClientState::default();
        let old_cs = cs.clone();

        // Dispute empty account
        cs.process_transaction(Transaction::Dispute {
            client: ClientId(1),
            tx: TransactionId(1),
        });
        assert_eq!(cs, old_cs);

        // Dispute deposit
        cs.process_transaction(Transaction::Deposit {
            client: ClientId(1),
            tx: TransactionId(1),
            amount: 3.,
        });
        cs.process_transaction(Transaction::Dispute {
            client: ClientId(1),
            tx: TransactionId(1),
        });
        assert_eq!(
            cs,
            ClientState {
                available: 0.,
                held: 3.,
                locked: false,
                deposits: maplit! {
                    TransactionId(1) =>
                    DepositState {
                        amount: 3.0,
                        ty: DepositStateType::Disputed
                    }
                }
            }
        );

        // Dispute already disputed.
        let old_cs = cs.clone();
        cs.process_transaction(Transaction::Dispute {
            client: ClientId(1),
            tx: TransactionId(1),
        });
        assert_eq!(cs, old_cs);

        // Deposit->withdraw->dispute results in negative available.
        cs.process_transaction(Transaction::Deposit {
            client: ClientId(1),
            tx: TransactionId(2),
            amount: 5.,
        });
        cs.process_transaction(Transaction::Withdrawal {
            client: ClientId(1),
            tx: TransactionId(3),
            amount: 5.,
        });
        cs.process_transaction(Transaction::Dispute {
            client: ClientId(1),
            tx: TransactionId(2),
        });
        assert_eq!(
            cs,
            ClientState {
                available: -5.,
                held: 8.,
                locked: false,
                deposits: maplit! {
                    TransactionId(1) =>
                    DepositState {
                        amount: 3.0,
                        ty: DepositStateType::Disputed
                    },
                    TransactionId(2) =>
                    DepositState {
                        amount: 5.0,
                        ty: DepositStateType::Disputed
                    }
                }
            }
        );
        // Dispute withdrawal
        cs.process_transaction(Transaction::Deposit {
            client: ClientId(1),
            tx: TransactionId(5),
            amount: 5.,
        });
        cs.process_transaction(Transaction::Withdrawal {
            client: ClientId(1),
            tx: TransactionId(6),
            amount: 5.,
        });

        let old_cs = cs.clone();
        cs.process_transaction(Transaction::Dispute {
            client: ClientId(1),
            tx: TransactionId(6),
        });
        assert_eq!(cs, old_cs)
    }

    #[test]
    fn test_resolve() {
        let mut cs = ClientState::default();
        let old_cs = cs.clone();

        // Resolve empty account
        cs.process_transaction(Transaction::Resolve {
            client: ClientId(1),
            tx: TransactionId(1),
        });
        assert_eq!(cs, old_cs);

        // Resolve deposit
        cs.process_transaction(Transaction::Deposit {
            client: ClientId(1),
            tx: TransactionId(1),
            amount: 3.,
        });
        let old_cs = cs.clone();
        cs.process_transaction(Transaction::Resolve {
            client: ClientId(1),
            tx: TransactionId(1),
        });
        assert_eq!(cs, old_cs);

        // Resolve dispute
        cs.process_transaction(Transaction::Dispute {
            client: ClientId(1),
            tx: TransactionId(1),
        });
        cs.process_transaction(Transaction::Resolve {
            client: ClientId(1),
            tx: TransactionId(1),
        });
        assert_eq!(cs, old_cs);

        // Resolve chargeback
        cs.process_transaction(Transaction::Dispute {
            client: ClientId(1),
            tx: TransactionId(1),
        });
        cs.process_transaction(Transaction::ChargeBack {
            client: ClientId(1),
            tx: TransactionId(1),
        });
        cs.process_transaction(Transaction::Resolve {
            client: ClientId(1),
            tx: TransactionId(1),
        });

        assert_eq!(
            cs,
            ClientState {
                available: 0.,
                held: 0.,
                locked: true,
                deposits: maplit! {
                    TransactionId(1) =>
                    DepositState {
                        amount: 3.0,
                        ty: DepositStateType::ChargedBack
                    }
                }
            }
        );
    }

    #[test]
    fn test_chargeback() {
        let mut cs = ClientState::default();
        let old_cs = cs.clone();

        // Chargeback empty account
        cs.process_transaction(Transaction::ChargeBack {
            client: ClientId(1),
            tx: TransactionId(1),
        });
        assert_eq!(cs, old_cs);

        // Chargeback deposit
        cs.process_transaction(Transaction::Deposit {
            client: ClientId(1),
            tx: TransactionId(1),
            amount: 3.,
        });
        let old_cs = cs.clone();
        cs.process_transaction(Transaction::ChargeBack {
            client: ClientId(1),
            tx: TransactionId(1),
        });
        assert_eq!(cs, old_cs);

        // Chargeback disputed
        cs.process_transaction(Transaction::Dispute {
            client: ClientId(1),
            tx: TransactionId(1),
        });
        cs.process_transaction(Transaction::ChargeBack {
            client: ClientId(1),
            tx: TransactionId(1),
        });
        assert_eq!(
            cs,
            ClientState {
                available: 0.,
                held: 0.,
                locked: true,
                deposits: maplit! {
                    TransactionId(1) =>
                    DepositState {
                        amount: 3.0,
                        ty: DepositStateType::ChargedBack
                    }
                }
            }
        );

        // After a chargeback, the client is locked so transactions
        // should not alter the state.
        let old_cs = cs.clone();
        // Deposit after ChargeBack
        cs.process_transaction(Transaction::Deposit {
            client: ClientId(1),
            tx: TransactionId(4),
            amount: 8.,
        });
        assert_eq!(cs, old_cs);

        // Withdraw after ChargeBack
        cs.process_transaction(Transaction::Withdrawal {
            client: ClientId(1),
            tx: TransactionId(5),
            amount: 8.,
        });
        assert_eq!(cs, old_cs);

        // Dispute after ChargeBack
        cs.process_transaction(Transaction::Dispute {
            client: ClientId(1),
            tx: TransactionId(1),
        });
        assert_eq!(cs, old_cs);

        // Resolve after ChargeBack
        cs.process_transaction(Transaction::Resolve {
            client: ClientId(1),
            tx: TransactionId(1),
        });
        assert_eq!(cs, old_cs);

        // ChargeBack after ChargeBack
        cs.process_transaction(Transaction::ChargeBack {
            client: ClientId(1),
            tx: TransactionId(1),
        });
        assert_eq!(cs, old_cs);
    }

    #[test]
    fn test_chargeback_withdrawal() {
        let mut cs = ClientState::default();

        // Deposit->withdraw->dispute->chargeback leads to
        // negative available
        cs.process_transaction(Transaction::Deposit {
            client: ClientId(1),
            tx: TransactionId(1),
            amount: 3.0,
        });
        cs.process_transaction(Transaction::Withdrawal {
            client: ClientId(1),
            tx: TransactionId(2),
            amount: 2.0,
        });
        cs.process_transaction(Transaction::Dispute {
            client: ClientId(1),
            tx: TransactionId(1),
        });
        cs.process_transaction(Transaction::ChargeBack {
            client: ClientId(1),
            tx: TransactionId(1),
        });
        println!("cs:{:?};",cs);
        assert_eq!(
            cs,
            ClientState {
                available: -2.,
                held: 0.,
                locked: true,
                deposits: maplit! {
                    TransactionId(1) =>
                    DepositState {
                        amount: 3.0,
                        ty: DepositStateType::ChargedBack
                    }
                }
            }
        );
    }
    use better_macro::println;
}
