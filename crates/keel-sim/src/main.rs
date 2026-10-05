//! `keel-sim --seeds 100000` runs seeds in parallel and reports invariant violations.
//! `keel-sim --seed 4242 --verbose` reproduces one seed exactly.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use clap::Parser;
use keel_sim::{SimOptions, SimReport, run_seed};

#[derive(Parser)]
#[command(name = "keel-sim", about = "KEEL deterministic simulation testing")]
struct Args {
    /// Number of seeds to run.
    #[arg(long, default_value_t = 1000)]
    seeds: u64,
    /// First seed.
    #[arg(long, default_value_t = 0)]
    start: u64,
    /// Run exactly this seed (overrides --seeds/--start).
    #[arg(long)]
    seed: Option<u64>,
    /// Run every seed twice and require identical histories.
    #[arg(long)]
    check_determinism: bool,
    #[arg(long, default_value_t = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4))]
    threads: usize,
    #[arg(long)]
    verbose: bool,
    /// Write every report as JSON lines to this file.
    #[arg(long)]
    out: Option<std::path::PathBuf>,
}

fn main() {
    let args = Args::parse();
    let opts = SimOptions::default();

    if let Some(seed) = args.seed {
        let r = run_seed(seed, opts);
        println!("{}", serde_json::to_string_pretty(&r).unwrap());
        if args.check_determinism {
            let again = run_seed(seed, opts);
            assert_eq!(r.head_hash, again.head_hash, "seed {seed} is not deterministic");
            println!("deterministic: yes");
        }
        std::process::exit(if r.violations.is_empty() { 0 } else { 1 });
    }

    let started = Instant::now();
    let next = Arc::new(AtomicU64::new(args.start));
    let end = args.start + args.seeds;
    let reports: Arc<Mutex<Vec<SimReport>>> = Arc::new(Mutex::new(Vec::with_capacity(args.seeds as usize)));
    let nondeterministic: Arc<Mutex<Vec<u64>>> = Arc::default();
    let done = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..args.threads)
        .map(|_| {
            let (next, reports, nondet, done) = (next.clone(), reports.clone(), nondeterministic.clone(), done.clone());
            let check = args.check_determinism;
            let total = args.seeds;
            std::thread::spawn(move || {
                loop {
                    let seed = next.fetch_add(1, Ordering::SeqCst);
                    if seed >= end {
                        break;
                    }
                    let r = run_seed(seed, opts);
                    if check && run_seed(seed, opts).head_hash != r.head_hash {
                        nondet.lock().unwrap().push(seed);
                    }
                    reports.lock().unwrap().push(r);
                    let d = done.fetch_add(1, Ordering::SeqCst) + 1;
                    if d % 10_000 == 0 {
                        eprintln!("  {d}/{total} seeds");
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().expect("sim thread panicked");
    }

    let reports = reports.lock().unwrap();
    if let Some(path) = &args.out {
        let lines: Vec<String> = reports.iter().map(|r| serde_json::to_string(r).unwrap()).collect();
        std::fs::write(path, lines.join("\n") + "\n").expect("write --out");
    }

    let mut by_tier: BTreeMap<(String, String), u64> = BTreeMap::new();
    let (mut kills, mut zombies, mut stale, mut forks) = (0u64, 0u64, 0u64, 0u64);
    let mut failing: Vec<&SimReport> = Vec::new();
    for r in reports.iter() {
        *by_tier.entry((r.tier.clone(), r.status.clone())).or_default() += 1;
        kills += u64::from(r.kills);
        zombies += u64::from(r.zombies);
        stale += r.stale_rejections;
        forks += u64::from(r.forked);
        if !r.violations.is_empty() {
            failing.push(r);
        }
        if args.verbose {
            println!("{}", serde_json::to_string(r).unwrap());
        }
    }
    failing.sort_by_key(|r| r.seed);

    println!("KEEL DST: {} seeds in {:.1}s", reports.len(), started.elapsed().as_secs_f64());
    println!(
        "  faults injected: {kills} worker kills, {zombies} zombie workers, {stale} stale-epoch writes rejected, {forks} forks"
    );
    for ((tier, status), n) in &by_tier {
        println!("  tier {tier}: {n:>7} {status}");
    }
    let nondet = nondeterministic.lock().unwrap();
    if !nondet.is_empty() {
        println!("  NON-DETERMINISTIC seeds: {:?}", &nondet[..nondet.len().min(20)]);
    }
    if failing.is_empty() && nondet.is_empty() {
        println!("  invariant violations: 0");
    } else {
        println!("  invariant violations: {} seeds", failing.len());
        for r in failing.iter().take(20) {
            println!("    seed {} (tier {}): {}", r.seed, r.tier, r.violations.join("; "));
        }
        println!("  reproduce with: keel-sim --seed <seed> --verbose");
        std::process::exit(1);
    }
}
