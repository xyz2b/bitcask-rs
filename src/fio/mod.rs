pub mod file_io;

use std::path::PathBuf;

use file_io::FileIO;

use crate::errors::Result;

/// 抽象 IO 管理接口，可以接入不同的 IO 类型，目前支持标准文件 IO
pub trait IOManager: Sync + Send {
    /// 从文件给定位置读取对应的数据
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
    /// 写入字节数组到文件
    fn write(&self, buf: &[u8]) -> Result<usize>;
    /// 持久化数据
    fn sync(&self) -> Result<()>;
}

pub fn new_io_manager(file_name: PathBuf) -> Result<impl IOManager> {
    FileIO::new(file_name)
}
