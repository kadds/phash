use crate::{any_as_u8_mut_slice, any_as_u8_slice, PHashValueDeserializer, PHashValueSerializer};

#[derive(Default)]
#[repr(packed)]
struct DefaultHeader {
    count: u64,
}

#[derive(Default)]
pub struct DefaultHashValueWriter {}

impl DefaultHashValueWriter {
    pub fn new() -> Self {
        Self {}
    }
}

impl PHashValueSerializer for DefaultHashValueWriter {
    fn write_all<W>(&self, values: &Vec<&[u8]>, writer: &mut W) -> Option<()>
    where
        W: std::io::Write,
    {
        let header = DefaultHeader {
            count: values.len() as u64,
        };
        unsafe {
            writer.write(any_as_u8_slice(&header)).unwrap();
        }

        let mut sum = 0u32;
        for value in values {
            let new_sum = value.len().checked_add(sum as usize)?;
            if new_sum >= u32::MAX as usize {
                return None;
            }
            sum = new_sum as u32;
            unsafe {
                writer.write(any_as_u8_slice(&sum)).unwrap();
            }
        }

        for value in values {
            writer.write(value).unwrap();
        }

        Some(())
    }
}

pub struct DefaultHashValueReader {
    header: DefaultHeader,
    index_ptr: *const u32,
    content_ptr: *const u8,
}

impl DefaultHashValueReader {
    pub fn new() -> Self {
        Self {
            header: DefaultHeader::default(),
            index_ptr: std::ptr::null(),
            content_ptr: std::ptr::null(),
        }
    }
}

impl PHashValueDeserializer for DefaultHashValueReader {
    fn get<'a>(&'a self, index: crate::HashIndex) -> &'a [u8] {
        debug_assert!(index < self.header.count as crate::HashIndex);
        unsafe {
            let offset = *self.index_ptr.add(index as usize);
            if index > 0 {
                let offset_prev = *self.index_ptr.add(index as usize - 1);
                let size = offset - offset_prev;
                std::slice::from_raw_parts(
                    self.content_ptr.add(offset_prev as usize),
                    size as usize,
                )
            } else {
                std::slice::from_raw_parts(self.content_ptr, offset as usize)
            }
        }
    }
    fn load<'a>(&'a mut self, ptr: &'a [u8]) -> Option<()> {
        unsafe {
            let desc = any_as_u8_mut_slice(&mut self.header);
            std::ptr::copy(ptr.as_ptr(), desc.as_mut_ptr(), desc.len());
            self.index_ptr = ptr.as_ptr().add(std::mem::size_of::<DefaultHeader>()) as *const u32;
            self.content_ptr = self.index_ptr.add(self.header.count as usize) as *const u8;
        }
        Some(())
    }
}
