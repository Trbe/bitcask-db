#![allow(dead_code)]

use bitcask::Bitcask;
use bytes::Bytes;
use rand::{
    distributions::{Distribution, Standard, Uniform},
    Rng,
};
use tempfile::TempDir;

const ITER: usize = 10000;
const KEY_SIZE: usize = 1024;
const VAL_SIZE: usize = 8096;

#[derive(Clone)]
struct KeyValuePair(Bytes, Bytes);

impl KeyValuePair {
    fn random<R: Rng>(rng: &mut R, key_size: usize, val_size: usize) -> KeyValuePair {
        let key: Bytes = rng.sample_iter(Standard).take(key_size).collect();
        let val: Bytes = rng.sample_iter(Standard).take(val_size).collect();
        KeyValuePair(key, val)
    }

    fn random_many<R: Rng>(
        rng: &mut R,
        size: usize,
        key_size: usize,
        val_size: usize,
    ) -> Vec<KeyValuePair> {
        let key_dist = Uniform::from(1..key_size);
        let val_dist = Uniform::from(1..val_size);
        (0..size)
            .map(|_| {
                let ksz = key_dist.sample(rng);
                let vsz = val_dist.sample(rng);
                KeyValuePair::random(rng, ksz, vsz)
            })
            .collect()
    }
}

fn get_bitcask() -> (Bitcask, TempDir) {
    let tmpdir = TempDir::new().unwrap();
    let bitcask = Bitcask::open(tmpdir.path()).unwrap();
    (bitcask, tmpdir)
}