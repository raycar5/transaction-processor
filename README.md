# Transaction Processor
Code challenge to implement a simple transaction processor that takes a csv with transactions as input
and outputs another csv with account states through stdout.

## Running
### Processing

To process inputs you can run in 2 modes (remember to run `gen.sh` first):

1. Single threaded: `cargo run --release inputs/big/random.csv > result.csv`
2. Multi threaded: `cargo run --features multithreaded --release inputs/big/random.csv > result.csv`

You can also enable verbose output of the invalid transactions with `--features stderr` but it slows down performance considerably so it should only be used in smaller inputs like `cargo run --features stderr --release inputs/invalid.csv > result.csv`.

### Generating
You can generate input csv data with 2 commands, both of them generate 10,000,000 transactions.

1. Random `cargo run --release inputs/big/random.csv genrandom`
2. "Smart" Random `cargo run --release inputs/big/random.csv gen` or `gen.sh`

The weird ordering of the arguments is because of challenge constraints.

## Docs
You can generate documentation by running `doc.sh`, it will automatically open in a browser tab.

## Benchmarking
You can time the execution of the multi threaded and single threaded processing with `time_multi.sh` and `time.sh` respectively.

## Profiling
You can generate a flamegraph profile of the single threaded implementation by running `profile.sh`, it requires that you have previously ran `cargo install flamegraph`.
Remember adding
```toml
[profile.release]
debug = true
```
to `Cargo.toml` or the flamegraph will not have the debug symbols.

## Testing
Run `cargo test` to execute unit tests.

## Development decisions
1. From the start I chose the async approach because from experience it often ends up utilizing system resources better.
2. In the beginning I opted for [async-std] as the runtime because [tokio](https://github.com/tokio-rs/tokio) did not support [csv-async].
3. Also in the beginning I used [csv-async] + [serde] for both serialization and deserialization because of simplicity.
4. After I had verified that the processing worked correctly, I started searching for bottlenecks and found the first one in the runtime, I switched to [smol] which doubled performance.
5. After switching runtimes, it became apparent in the flamegraph that there were 2 distinct operations that could happen in parallel, parsing and processing (mostly hashmap lookups), so I implemented the multi threaded version and switched to a faster hashmap.
6. While testing the multi threaded implementation, initially it was slower, but I later found out it was because with enough (10 mil) transactions, if there is an even distribution, most accounts end up locked and therefore the processing step ends up being a single hashmap lookup. I then reduced the probability of the `chargeback` transactions in the generator which made the multi threaded approach faster. 
7. The next bottleneck seemed to be parsing so I switched to a custom parser, this also allowed me to switch the transaction representation to a better one, from 
```rust
struct Transaction{
    ty: TransactionType,
    client: ClientId,
    tx: TransactionId,
    amount: Option<f64>
}
```
to
```rust
pub enum Transaction {
    Deposit {
        client: ClientId,
        tx: TransactionId,
        amount: f64,
    },
    Dispute {
        client: ClientId,
        tx: TransactionId,
    },
    etc...
}
```
Parsing is one of the most dangerous steps in computing, so if this was a production system, a lot more testing and scrutiny would have to go into the parser as well as its lower level dependencies [atoi](https://github.com/pacman82/atoi-rs), [fast-float](https://github.com/aldanor/fast-float-rust) [parse-display](https://github.com/frozenlib/parse-display) and of course the [smol] runtime itself.

8. If I had more time I would try to parallelize the parsing step by partitioning the file by lines, and feeding a chunk of lines to each task.


[csv-async]:https://github.com/gwierzchowski/csv-async
[async-std]:https://github.com/async-rs/async-std
[serde]:https://github.com/serde-rs/serde
[smol]:https://github.com/smol-rs/smol