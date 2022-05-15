use std::hash::Hash;
use std::marker::PhantomData;
use std::num::Wrapping;

use crate::{any_array_as_u8_slice, any_as_u8_slice, HashIndexSerializeInfo, Hasher};
use crate::{any_as_u8_mut_slice, HashIndex, PHashIndexDeserializer, PHashIndexSerializer};

#[derive(Debug, Clone)]
pub struct CHDGeneratorConfig {
    pub bucket_element: u32,
    pub load_factor: f32,
    pub minimal: bool,
    pub retry: u32,
}

impl Default for CHDGeneratorConfig {
    fn default() -> Self {
        Self {
            bucket_element: 5,
            load_factor: 0.99f32,
            minimal: false,
            retry: 3,
        }
    }
}

impl CHDGeneratorConfig {
    pub fn bucket_element(mut self, bucket_element: u32) -> Self {
        self.bucket_element = bucket_element;
        self
    }
    pub fn load_factor(mut self, load_factor: f32) -> Self {
        self.load_factor = load_factor;
        self
    }
    pub fn minimal(mut self, minimal: bool) -> Self {
        self.minimal = minimal;
        self
    }
    pub fn retry(mut self, retry: u32) -> Self {
        self.retry = retry;
        self
    }
}

pub struct CHDGenerator<H> {
    reader: Option<CHDReader<H>>,
    mapping: Vec<u8>,
    config: CHDGeneratorConfig,
}

#[derive(Debug, Default, Clone)]
struct Bucket {
    index: u32,
    hashes: Vec<(u32, u32)>,
}

impl<H> CHDGenerator<H> {
    pub fn new() -> Self {
        Self {
            reader: None,
            mapping: Vec::new(),
            config: CHDGeneratorConfig::default(),
        }
    }
    pub fn from_config(config: CHDGeneratorConfig) -> Self {
        Self {
            reader: None,
            mapping: Vec::new(),
            config,
        }
    }
}

#[derive(Default)]
#[repr(packed)]
#[allow(unused)]
struct Header {
    flag: u32,
    table_size: u32,
    bucket_size: u32,
}

struct KeyHash {
    h: u32,
    h0: u32,
    h1: u32,
}

fn key_hash<K: Hash, H: Hasher>(k: &K, bucket_size: u32, table_size: u32) -> KeyHash {
    let mut hasher = H::default();
    k.hash(&mut hasher);
    let hash = hasher.finish_u128();

    let h = (hash >> 64) as u32 % bucket_size;
    let h0 = (((hash >> 32) as u32) % table_size) as u32;
    let h1 = (((hash & 0xFFFFFFFF) as u32) % table_size) as u32;
    KeyHash { h, h0, h1 }
}

#[inline]
fn displace(h0: u32, h1: u32, d0: u32, d1: u32) -> u32 {
    (Wrapping(h0) + (Wrapping(h1) * Wrapping(d1)) + Wrapping(d0)).0
}

impl<H> CHDGenerator<H>
where
    H: Hasher,
{
    fn try_generate<'a,K>(&mut self, keys: &Vec<&'a K>, table_size: u32, bucket_size: u32)  -> Option<(Header, Vec<u32>)>
    where K: Hash {
        let mut buckets = Vec::<Bucket>::new();
        buckets.resize(bucket_size as usize, Bucket::default());
        for key in keys {
            let key_hash = key_hash::<K, H>(key, bucket_size, table_size);
            buckets[key_hash.h as usize].index = key_hash.h;
            buckets[key_hash.h as usize]
                .hashes
                .push((key_hash.h0, key_hash.h1));
        }

        buckets.sort_by(|a, b| b.hashes.len().cmp(&a.hashes.len()));

        let mut used = bitvec::vec::BitVec::<usize>::new();
        used.resize(table_size as usize, false);

        let max_len_of_hashes = buckets[0].hashes.len();
        let mut pushed = Vec::with_capacity(max_len_of_hashes);

        let max_hash_func = u32::min(table_size * table_size, 1 << 24);

        let mut result = Vec::new();
        result.resize(table_size as usize, 0 as u32);

        // displace all
        for bucket in &mut buckets {
            if bucket.hashes.len() == 0 {
                continue;
            }
            let mut hash_func = 0;
            let mut d0 = 0u32;
            let mut d1 = 0u32;
            let mut ok = false;
            while !ok {
                ok = true;
                pushed.resize(0, 0);
                for (h0, h1) in &bucket.hashes {
                    let h0 = *h0;
                    let h1 = *h1;
                    let final_hash = displace(h0, h1, d0, d1) % table_size;
                    let pos = final_hash as usize;
                    unsafe {
                        if *used.get_unchecked(pos) {
                            for idx in &pushed {
                                used.set_unchecked(*idx as usize, false);
                            }
                            pushed.clear();
                            ok = false;
                            break;
                        }
                        used.set_unchecked(pos, true);
                    }
                    pushed.push(final_hash);
                }
                if !ok {
                    hash_func += 1;
                    d1 += 1;
                    if d1 >= table_size {
                        d1 = 0;
                        d0 += 1;
                    }
                    if hash_func >= max_hash_func {
                        return None;
                    }
                } else {
                    result[bucket.index as usize] = hash_func as u32;
                }
            }
        }

        let header = Header {
            flag: 0,
            table_size,
            bucket_size,
        };
        Some((header, result))
    }
}

impl<K, H> PHashIndexSerializer<K, H> for CHDGenerator<H>
where
    H: Hasher,
    K: Hash,
{
    type Deserializer = CHDReader<H>;
    fn generate<'a, W>(
        &mut self,
        keys: &Vec<&'a K>,
        writer: &mut W,
    ) -> Option<HashIndexSerializeInfo>
    where
        W: std::io::Write + std::io::Seek,
    {
        assert!(self.config.bucket_element >= 1 && self.config.bucket_element <= 1000);
        assert!(self.config.load_factor <= 1.0f32 && self.config.load_factor >= 0.05f32);

        let mut table_size = (keys.len() as f32 / self.config.load_factor) as u32;
        if self.config.minimal {
            table_size = keys.len() as u32;
        }

        let bucket_size = (keys.len() as u32 + self.config.bucket_element - 1) / self.config.bucket_element;


        let (header, result) = loop {
            if self.config.retry == 0 {
                return None;
            }
            if let Some(v) =  self.try_generate(&keys, table_size, bucket_size) {
                break v
            }
            table_size += 1;
            self.config.retry -= 1;
        };

        unsafe {
            writer.write(any_as_u8_slice(&header)).unwrap();
            writer
                .write(any_array_as_u8_slice(result.as_slice()))
                .unwrap();
        }

        self.mapping.resize(result.as_slice().len() * 4, 0);
        unsafe {
            self.mapping
                .copy_from_slice(any_array_as_u8_slice(result.as_slice()));
        }

        let reader = CHDReader::with(self.mapping.as_ptr() as *const u32, header);
        self.reader = Some(reader);

        Some(HashIndexSerializeInfo {
            max_hash_index: table_size,
        })
    }

    fn pick(&self, key: &K) -> HashIndex {
        self.reader.as_ref().unwrap().get_hash_index(key)
    }
}

pub struct CHDReader<H> {
    header: Header,
    ptr: *const u32,
    _pd0: PhantomData<H>,
}

impl<H> CHDReader<H> {
    pub fn new() -> Self {
        Self {
            ptr: std::ptr::null(),
            header: Header::default(),
            _pd0: PhantomData::default(),
        }
    }

    fn with(ptr: *const u32, header: Header) -> Self {
        Self {
            ptr,
            header,
            _pd0: PhantomData::default(),
        }
    }
}

impl<K, H> PHashIndexDeserializer<K, H> for CHDReader<H>
where
    H: Hasher,
    K: Hash,
{
    type Serializer = CHDGenerator<H>;
    fn load<'a>(&'a mut self, ptr: &'a [u8]) -> Option<()> {
        unsafe {
            let desc = any_as_u8_mut_slice(&mut self.header);
            std::ptr::copy(ptr.as_ptr(), desc.as_mut_ptr(), desc.len());
            self.ptr = ptr.as_ptr().add(std::mem::size_of::<Header>()) as *const u32;
        }
        Some(())
    }
    fn get_hash_index(&self, key: &K) -> HashIndex {
        let key_hash = key_hash::<K, H>(key, self.header.bucket_size, self.header.table_size);

        let h = key_hash.h;
        let h0 = key_hash.h0;
        let h1 = key_hash.h1;

        let hash_func = unsafe { *self.ptr.add(h as usize) } as u32;

        let table_size = self.header.table_size;

        let d0 = hash_func / table_size;
        let d1 = hash_func % table_size;

        displace(h0, h1, d0, d1) % table_size
    }
}
