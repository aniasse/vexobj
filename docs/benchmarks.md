# Benchmarks

A self-hosted object store is only "high-performance" if there are
actual numbers behind the claim. This page lists the micro-benchmarks
we ship, how to run them, and what the current cost looks like on a
representative machine.

## How to run

```bash
# Component micro-benchmarks (criterion, single-threaded)
cargo bench -p vexobj-storage   --bench hot_paths
cargo bench -p vexobj-s3-compat --bench sigv4

# End-to-end HTTP load (requires a running server + curl)
./bench/bench.sh http://localhost:8000 "$VEXOBJ_KEY" 20 500
```

Criterion reports throughput for the byte-in / byte-out paths and
nanoseconds per call for the request-level paths. It automatically
runs a warm-up phase and reports the 99% confidence interval.

## Results

Hardware: Intel Core i5-10300H (8 threads, 2.5 GHz base, no Intel
SHA-NI), Linux 6.17, Rust 1.89, `release` profile.

### SHA-256 (hashing plaintext on PUT)

| Payload | Time     | Throughput  |
|---------|----------|-------------|
| 4 KB    | 18.5 µs  | ~210 MB/s   |
| 64 KB   | 265 µs   | ~236 MB/s   |
| 1 MB    | 4.18 ms  | ~239 MB/s   |
| 16 MB   | 65.9 ms  | ~243 MB/s   |

SHA-256 is the bottleneck on CPUs without Intel SHA-NI (pre-Ice Lake,
pre-Zen 1). AMD Ryzen and newer Intel chips roughly 4–5× these
numbers.

### AES-256-GCM (SSE encrypt / decrypt)

| Payload | Encrypt time | Encrypt thrpt | Decrypt time | Decrypt thrpt |
|---------|--------------|---------------|--------------|---------------|
| 4 KB    | 5.9 µs       | ~660 MB/s     | 5.9 µs       | ~670 MB/s     |
| 64 KB   | 51.0 µs      | ~1.22 GB/s    | 50.8 µs      | ~1.22 GB/s    |
| 1 MB    | 776 µs       | ~1.26 GB/s    | 786 µs       | ~1.24 GB/s    |
| 16 MB   | 14.0 ms      | ~1.14 GB/s    | 16.5 ms      | ~0.97 GB/s    |

Hardware AES instructions (AES-NI) kick in and the encrypt path is
roughly 5× faster than our SHA-256 path. For most workloads, turning
SSE on is not the thing that slows you down — the hash is.

### SigV4 verification (per S3 request)

| Operation                  | Time      | Rate               |
|----------------------------|-----------|--------------------|
| Parse `Authorization:` header | 303 ns | ~3.3 M ops/s/core  |
| Verify a full request      | 9.96 µs   | ~100 k req/s/core  |

Each `/s3/*` request costs about 10 µs of CPU before it touches the
storage engine. A single core sustains ~100 k SigV4 verifications per
second, so 8 cores ≈ 800 k/s — comfortably above any realistic
request rate for a single-node deployment.

## Reading these numbers

- **SSE has no visible cost** for most workloads. AES-GCM runs on AES-NI
  at over a GB/s; the SQLite write and the disk syscall dominate.
- **SHA-256 limits single-object PUT throughput** on CPUs without
  SHA-NI. A 100 MB upload spends ~420 ms just hashing on this machine.
  If you need to saturate a 10 GbE link, pick AMD or a recent Intel.
- **SigV4 is not a hot path** yet. If you ever see requests queueing on
  signature verification, check `sha2`'s feature flags — enabling the
  `asm` build gives an instant win on older hardware.

## Regressing on these numbers

Criterion writes its history to `target/criterion/`. A simple way to
catch regressions during development:

```bash
git checkout main && cargo bench -p vexobj-storage --bench hot_paths
git checkout my-branch && cargo bench -p vexobj-storage --bench hot_paths
# criterion prints change detection automatically
```

The CI `release` workflow could be extended to fail on >10% regressions
by gating on `cargo-benchcmp` output — not wired up yet.
