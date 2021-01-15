cargo build --features multithreaded --release
perf stat -B -e cache-references,cache-misses,cycles,instructions,branches,branch-misses target/release/transaction_processor inputs/big/random.csv >result.csv
