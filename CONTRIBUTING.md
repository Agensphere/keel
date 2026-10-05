# Contributing

Thanks for helping. KEEL's product is a correctness claim, so every change has to keep these checks green:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace                                   # set KEEL_TEST_DATABASE_URL to include Postgres
cargo run --release -p keel-sim -- --seeds 10000         # DST: must report 0 invariant violations
```

If a DST seed fails, include `keel-sim --seed N --verbose` output in your PR.

## Developer Certificate of Origin

Every commit must be signed off (`git commit -s`), certifying the [DCO](https://developercertificate.org/):

```
Signed-off-by: Your Name <you@example.com>
```

## Replay compatibility

Changing anything that affects fingerprints, step ids, event bodies or hashing can strand in-flight runs. Call it out in the PR. Changes to the history format require a major version and a replay shim.
