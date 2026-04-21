//! Micro-benchmarks for the hot paths inside the storage engine:
//! SHA-256 hashing and AES-256-GCM encryption. The PUT path hashes
//! every byte and — when SSE is on — encrypts every byte; these two
//! together dominate CPU cost for uploads.
//!
//! Run with: `cargo bench -p vaultfs-storage`

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use sha2::{Digest, Sha256};
use vexobj_storage::Encryptor;

const SIZES: &[usize] = &[4 * 1024, 64 * 1024, 1024 * 1024, 16 * 1024 * 1024];

fn sha256_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("sha256");
    for &size in SIZES {
        let data = vec![0xA5u8; size];
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &data, |b, data| {
            b.iter(|| {
                let mut h = Sha256::new();
                h.update(black_box(data));
                black_box(h.finalize());
            });
        });
    }
    group.finish();
}

fn encrypt_throughput(c: &mut Criterion) {
    let key = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";
    let enc = Encryptor::from_hex(key).unwrap();
    let sha = "deadbeef".repeat(8); // 64-char fake sha, deterministic

    let mut group = c.benchmark_group("aes256gcm_encrypt");
    for &size in SIZES {
        let data = vec![0x5Au8; size];
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &data, |b, data| {
            b.iter(|| {
                black_box(enc.encrypt(black_box(&sha), black_box(data)).unwrap());
            });
        });
    }
    group.finish();
}

fn decrypt_throughput(c: &mut Criterion) {
    let key = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";
    let enc = Encryptor::from_hex(key).unwrap();
    let sha = "deadbeef".repeat(8);

    let mut group = c.benchmark_group("aes256gcm_decrypt");
    for &size in SIZES {
        let plain = vec![0x5Au8; size];
        let ciphertext = enc.encrypt(&sha, &plain).unwrap();
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            &ciphertext,
            |b, ciphertext| {
                b.iter(|| {
                    black_box(enc.decrypt(black_box(&sha), black_box(ciphertext)).unwrap());
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    sha256_throughput,
    encrypt_throughput,
    decrypt_throughput
);
criterion_main!(benches);
