use std::{fs::OpenOptions, path::PathBuf, sync::Arc};

use memmap2::Mmap;
use parking_lot::Mutex;

use crate::errors::{Errors, Result};
use log::error;

use super::IOManager;

pub struct MMapIO {
    map: Arc<Mutex<Mmap>>,
}

impl MMapIO {
    pub fn new(file_name: PathBuf) -> Result<Self> {
        match OpenOptions::new().read(true).open(file_name) {
            Ok(file) => {
                let map = unsafe { Mmap::map(&file).expect("failed to map the file") };

                return Ok(MMapIO {
                    map: Arc::new(Mutex::new(map)),
                });
            }
            Err(e) => {
                error!("open data file err: {}", e);
                return Err(Errors::FailedOpenDataFile);
            }
        }
    }
}

impl IOManager for MMapIO {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let map_arr = self.map.lock();
        let end = offset + buf.len() as u64;
        if end > map_arr.len() as u64 {
            return Err(Errors::ReadDataFileEof);
        }

        let val = &map_arr[offset as usize..end as usize];
        buf.copy_from_slice(val);

        Ok(val.len())
    }

    fn write(&self, _buf: &[u8]) -> Result<usize> {
        unimplemented!();
    }

    fn sync(&self) -> Result<()> {
        unimplemented!();
    }

    fn size(&self) -> u64 {
        let map_arr = self.map.lock();
        map_arr.len() as u64
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::fio::file_io::FileIO;

    use super::*;

    #[test]
    fn test_mmap_io_read() {
        let path = PathBuf::from("/tmp/mmap-test.data");
        let fio_res = FileIO::new(path.clone());
        assert!(fio_res.is_ok());
        let fio = fio_res.ok().unwrap();

        let mmap_io_res = MMapIO::new(path.clone());
        assert!(mmap_io_res.is_ok());
        let mmap_io = mmap_io_res.unwrap();

        let mut buf0 = [0u8; 5];
        let read_res0 = mmap_io.read(&mut buf0, 0);
        assert_eq!(read_res0.err().unwrap(), Errors::ReadDataFileEof);

        let res1 = fio.write("key-a".as_bytes());
        assert!(res1.is_ok());
        assert_eq!(5, res1.ok().unwrap());

        let res2 = fio.write("key-b".as_bytes());
        assert!(res2.is_ok());
        assert_eq!(5, res2.ok().unwrap());

        let mmap_io_res2 = MMapIO::new(path.clone());
        assert!(mmap_io_res2.is_ok());
        let mmap_io2 = mmap_io_res2.unwrap();

        let mut buf = [0u8; 5];
        let read_res1 = mmap_io2.read(&mut buf, 0);
        assert!(read_res1.is_ok());
        assert_eq!(5, read_res1.ok().unwrap());
        println!("{:?}", String::from_utf8(buf.to_vec()));

        let mut buf2 = [0u8; 5];
        let read_res2 = mmap_io2.read(&mut buf2, 5);
        assert!(read_res2.is_ok());
        assert_eq!(5, read_res2.ok().unwrap());
        println!("{:?}", String::from_utf8(buf2.to_vec()));

        let res3 = fs::remove_file(path.clone());
        assert!(res3.is_ok());
    }
}
