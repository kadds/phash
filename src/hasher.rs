pub trait Hasher: std::hash::Hasher + Default {
    fn finish_u128(&self) -> u128;
}

#[derive(Debug, Default)]
pub struct CityHash {
    inner: u128,
}

impl std::hash::Hasher for CityHash {
    fn write(&mut self, data: &[u8]) {
        let hash = naive_cityhash::cityhash128(data);
        self.inner = (self.inner << 1) ^ (((hash.hi as u128) << 64) | (hash.lo as u128))
    }

    fn finish(&self) -> u64 {
        self.inner as u64
    }
}

impl Hasher for CityHash {
    fn finish_u128(&self) -> u128 {
        self.inner
    }
}
