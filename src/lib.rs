use std::{fs::File, hash::Hash, marker::PhantomData};

pub mod chd;
pub mod hasher;
pub mod value;
pub use hasher::Hasher;

unsafe fn any_as_u8_slice<T: Sized>(p: &T) -> &[u8] {
    ::std::slice::from_raw_parts((p as *const T) as *const u8, ::std::mem::size_of::<T>())
}

unsafe fn any_as_u8_mut_slice<T: Sized>(p: &mut T) -> &mut [u8] {
    ::std::slice::from_raw_parts_mut((p as *mut T) as *mut u8, ::std::mem::size_of::<T>())
}

unsafe fn any_array_as_u8_slice<T: Sized>(p: &[T]) -> &[u8] {
    ::std::slice::from_raw_parts(
        p.as_ptr() as *const u8,
        ::std::mem::size_of::<T>() * p.len(),
    )
}

#[allow(unused)]
unsafe fn any_array_as_u8_mut_slice<T: Sized>(p: &mut [T]) -> &mut [u8] {
    ::std::slice::from_raw_parts_mut(
        p.as_mut_ptr() as *mut u8,
        ::std::mem::size_of::<T>() * p.len(),
    )
}

type HashIndex = u32;

pub struct HashIndexSerializeInfo {
    pub max_hash_index: u32,
}

pub trait PHashIndexSerializer<K, H: Hasher>
where
    K: Hash,
{
    type Deserializer;
    fn generate<'a, W>(
        &mut self,
        keys: &Vec<&'a K>,
        writer: &mut W,
    ) -> Option<HashIndexSerializeInfo>
    where
        W: std::io::Write + std::io::Seek;

    fn pick(&self, key: &K) -> HashIndex;
}

pub trait PHashIndexDeserializer<K, H: Hasher>
where
    K: Hash,
{
    type Serializer;
    fn load<'a>(&'a mut self, ptr: &'a [u8]) -> Option<()>;
    fn get_hash_index(&self, key: &K) -> HashIndex;
}

pub trait PHashIndexEncoding {}

pub trait PHashValueSerializer {
    fn write_all<W>(&self, values: &Vec<&[u8]>, writer: &mut W) -> Option<()>
    where
        W: std::io::Write;
}

pub trait PHashValueDeserializer {
    fn load<'a>(&'a mut self, ptr: &'a [u8]) -> Option<()>;
    fn get<'a>(&'a self, index: HashIndex) -> &'a [u8];
}

#[derive(Default)]
#[repr(packed)]
#[allow(unused)]
struct PerfectHashMapHeader {
    endian: u8,
    version: u8,
    _reserved0: u16,
    flag: u32,
    index_size: u64,
    value_size: u64,
}

pub struct PerfectHashMapSerializer<H, K, I, V>
where
    I: PHashIndexSerializer<K, H>,
    V: PHashValueSerializer,
    H: Hasher,
    K: Hash,
{
    index_serializer: I,
    value_serializer: V,
    _pd0: PhantomData<H>,
    _pd1: PhantomData<K>,
}

impl<H, K, I, V> PerfectHashMapSerializer<H, K, I, V>
where
    I: PHashIndexSerializer<K, H>,
    V: PHashValueSerializer,
    H: Hasher,
    K: Hash,
{
    pub fn new(index_serializer: I, value_serializer: V) -> Self {
        Self {
            index_serializer,
            value_serializer,
            _pd0: PhantomData::default(),
            _pd1: PhantomData::default(),
        }
    }

    pub fn write_to_file<'a, P>(&mut self, kvs: &Vec<(K, &'a [u8])>, path: P)
    where
        P: AsRef<std::path::Path>,
    {
        let file = File::options()
            .write(true)
            .truncate(true)
            .create(true)
            .read(true)
            .open(path.as_ref())
            .unwrap();
        self.write_to(kvs, file)
    }
    pub fn write_to<'a, W>(&mut self, kvs: &Vec<(K, &'a [u8])>, mut writer: W)
    where
        W: std::io::Write + std::io::Seek,
    {
        #[cfg(target_endian = "big")]
        let endian = 1u8;
        #[cfg(target_endian = "little")]
        let endian = 0u8;
        let mut header = PerfectHashMapHeader {
            endian,
            version: 0,
            _reserved0: 0,
            flag: 0,
            index_size: 0 as u64,
            value_size: 0 as u64,
        };
        let header_len = std::mem::size_of::<PerfectHashMapHeader>() as u64;
        writer.seek(std::io::SeekFrom::Start(header_len)).unwrap();

        let mut keys: Vec<&K> = kvs.iter().map(|v| &v.0).collect();

        let index_info = self.index_serializer.generate(&keys, &mut writer).unwrap();
        let index_size = writer.stream_position().unwrap() - header_len;

        // release keys memory
        keys.clear();

        let mut values = Vec::new();
        unsafe {
            values.resize(
                index_info.max_hash_index as usize,
                std::slice::from_raw_parts(std::ptr::null(), 0),
            );
        }

        let mut used = bitvec::vec::BitVec::<usize>::new();
        used.resize(index_info.max_hash_index as usize, false);

        for (key, value) in kvs {
            let idx = self.index_serializer.pick(key);
            unsafe {
                if *used.get_unchecked(idx as usize) {
                    panic!("oops {} {}", idx, index_info.max_hash_index);
                }
            }
            used.set(idx as usize, true);
            values[idx as usize] = value;
        }

        let _ = self
            .value_serializer
            .write_all(&values, &mut writer)
            .unwrap();

        let value_size = writer.stream_position().unwrap() - header_len - index_size;

        header.index_size = index_size as u64;
        header.value_size = value_size as u64;
        let pos = writer.stream_position().unwrap();

        writer.seek(std::io::SeekFrom::Start(0)).unwrap();
        unsafe {
            writer.write(any_as_u8_slice(&header)).unwrap();
        }
        writer.seek(std::io::SeekFrom::Start(pos)).unwrap();
        writer.flush().unwrap();
    }
}

#[allow(unused)]
struct PerfectHashMapDeserializerInner {
    file: File,
    mmap: memmap2::Mmap,
    header: PerfectHashMapHeader,
}

pub struct PerfectHashMapDeserializer<H, K, I, V>
where
    I: PHashIndexDeserializer<K, H>,
    V: PHashValueDeserializer,
    H: Hasher,
    K: Hash,
{
    index_deserializer: I,
    value_deserializer: V,
    _pd0: PhantomData<H>,
    _pd1: PhantomData<K>,
    inner: Option<PerfectHashMapDeserializerInner>,
}

impl<H, K, I, V> PerfectHashMapDeserializer<H, K, I, V>
where
    I: PHashIndexDeserializer<K, H>,
    V: PHashValueDeserializer,
    H: Hasher,
    K: Hash,
{
    pub fn new(index_deserializer: I, value_deserializer: V) -> Self {
        Self {
            index_deserializer,
            value_deserializer,
            inner: None,
            _pd0: PhantomData::default(),
            _pd1: PhantomData::default(),
        }
    }

    pub fn load_from_mmap_file<P>(&mut self, path: P) -> usize
    where
        P: AsRef<std::path::Path>,
    {
        let file = File::options().read(true).write(false).open(path).unwrap();
        let mmap = unsafe { memmap2::MmapOptions::new().map(&file).unwrap() };

        let header_len = std::mem::size_of::<PerfectHashMapHeader>();
        let slice = &mmap[..header_len];

        let mut header = PerfectHashMapHeader::default();
        unsafe {
            any_as_u8_mut_slice(&mut header).copy_from_slice(slice);
        }

        let beg = header_len as usize;
        let end = beg + header.index_size as usize;

        self.index_deserializer.load(&mmap[beg..end]);

        let beg = header_len as usize + header.index_size as usize;
        let end = beg + header.value_size as usize;
        self.value_deserializer.load(&mmap[beg..end]);
        self.inner = Some(PerfectHashMapDeserializerInner { file, mmap, header });

        end
    }

    pub fn get<'a>(&'a self, key: &K) -> &'a [u8] {
        let hash_index = self.index_deserializer.get_hash_index(key);
        let value = self.value_deserializer.get(hash_index);
        value
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::chd::*;
    use super::value::*;
    use super::*;
    use rand::{distributions::Alphanumeric, Rng};

    #[test]
    fn data_test() {
        let test_file = "./test.bin";
        let mut keys = Vec::new();
        {
            let mut rng = rand::thread_rng();
            let mut map = HashSet::new();
            let len = 1024;
            for _ in 0..len {
                loop {
                    let key_len = rng.gen_range(5..30);
                    let s: String = rand::thread_rng()
                        .sample_iter(&Alphanumeric)
                        .take(key_len)
                        .map(char::from)
                        .collect();
                    if map.contains(&s) {
                        continue;
                    }
                    map.insert(s.clone());
                    keys.push((s.clone(), s));
                    break;
                }
            }

            let mut serializer = PerfectHashMapSerializer::<hasher::CityHash, _, _, _>::new(
                CHDGenerator::new(),
                DefaultHashValueWriter::new(),
            );
            let tmp_keys: Vec<(&str, &[u8])> = keys
                .iter()
                .map(|v| (v.0.as_str(), v.1.as_bytes()))
                .collect();
            serializer.write_to_file(&tmp_keys, test_file);
        }
        {
            let mut deserializer = PerfectHashMapDeserializer::<hasher::CityHash, _, _, _>::new(
                CHDReader::new(),
                DefaultHashValueReader::new(),
            );
            deserializer.load_from_mmap_file(test_file);
            for (k, v) in keys {
                unsafe {
                    let value: &str = std::str::from_utf8_unchecked(deserializer.get(&k));
                    assert_eq!(value, v);
                }
            }
        }
        std::fs::remove_file(test_file).unwrap();
    }
}
