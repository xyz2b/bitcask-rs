use std::{path::PathBuf, sync::Arc};

use bytes::Buf;
use bytes::BytesMut;

use parking_lot::RwLock;
use prost::decode_length_delimiter;
use prost::length_delimiter_len;

use crate::errors::Errors;
use crate::options::IOType;
use crate::{
    errors::Result,
    fio::{self, new_io_manager},
};

use super::log_record::LogRecord;
use super::log_record::LogRecordPos;
use super::log_record::LogRecordType;
use super::log_record::{max_log_record_header_size, ReadLogRecord};

pub const DATA_FILE_NAME_SUFFIX: &str = ".data";
pub const HINT_FILE_NAME: &str = "hint-index";
pub const MERGE_FIN_FILE_NAME: &str = "merge-fin";
pub const SEQ_NO_FILE_NAME: &str = "seq-no";

/// 数据文件
pub struct DataFile {
    file_id: Arc<RwLock<u64>>,           // 数据文件 ID
    wirte_off: Arc<RwLock<u64>>,         // 当前写偏移，记录该数据文件写到哪个位置了
    io_manager: Box<dyn fio::IOManager>, // IO 管理接口
}

impl DataFile {
    // 创建或打开一个新的数据文件
    pub fn new(dir_path: PathBuf, file_id: u64, io_type: IOType) -> Result<DataFile> {
        // 根据 path 和 file_id 构造出来完整的文件名称
        let filename = get_data_file_name(dir_path, file_id);
        // 初始化 IO manager
        let io_manager = new_io_manager(filename, io_type);

        Ok(DataFile {
            file_id: Arc::new(RwLock::new(file_id)),
            wirte_off: Arc::new(RwLock::new(0)),
            io_manager: io_manager,
        })
    }

    // 新建或打开 hint 索引文件
    pub fn new_hint_file(dir_path: PathBuf) -> Result<DataFile> {
        let filename = dir_path.join(HINT_FILE_NAME);

        // 初始化 IO manager
        let io_manager = new_io_manager(filename, IOType::StandardFIO);

        Ok(DataFile {
            file_id: Arc::new(RwLock::new(0)),
            wirte_off: Arc::new(RwLock::new(0)),
            io_manager: io_manager,
        })
    }

    // 新建或打开标识 merge 完成的文件
    pub fn new_merge_fin_file(dir_path: PathBuf) -> Result<DataFile> {
        let filename = dir_path.join(MERGE_FIN_FILE_NAME);

        // 初始化 IO manager
        let io_manager = new_io_manager(filename, IOType::StandardFIO);

        Ok(DataFile {
            file_id: Arc::new(RwLock::new(0)),
            wirte_off: Arc::new(RwLock::new(0)),
            io_manager: io_manager,
        })
    }

    // 新建或打开存储事务序列号的文件
    pub fn new_seq_no_file(dir_path: PathBuf) -> Result<DataFile> {
        let filename = dir_path.join(SEQ_NO_FILE_NAME);

        // 初始化 IO manager
        let io_manager = new_io_manager(filename, IOType::StandardFIO);

        Ok(DataFile {
            file_id: Arc::new(RwLock::new(0)),
            wirte_off: Arc::new(RwLock::new(0)),
            io_manager: io_manager,
        })
    }

    pub fn get_write_off(&self) -> u64 {
        let read_guard = self.wirte_off.read();
        *read_guard
    }

    pub fn set_write_off(&self, offset: u64) {
        let mut write_guard = self.wirte_off.write();
        *write_guard = offset;
    }

    pub fn get_file_id(&self) -> u64 {
        let read_guard = self.file_id.read();
        *read_guard
    }

    // 根据 offset 从数据文件中读取一个 LogRecord
    pub fn read_log_record(&self, offset: u64) -> Result<ReadLogRecord> {
        // 先读取 header 部分的数据
        // 初始化 header 字节数组
        let mut header_buf = BytesMut::zeroed(max_log_record_header_size());

        self.io_manager.read(&mut header_buf, offset)?;

        // 取出 type，在第一个字节
        let rec_type = header_buf.get_u8();

        // 取出 key 和 value 的长度
        let key_size = decode_length_delimiter(&mut header_buf).unwrap();
        let value_size = decode_length_delimiter(&mut header_buf).unwrap();

        // 如果 key 和 value 均为空，则说明读取到了文件末尾，直接返回
        if key_size == 0 && value_size == 0 {
            return Err(Errors::ReadDataFileEof);
        }

        // 获取实际的 header 大小
        let actual_header_size =
            length_delimiter_len(key_size) + length_delimiter_len(value_size) + 1;

        // 读取实际的 key 和 value，最后 4 个字节是 CRC 校验值
        let mut kv_buf = BytesMut::zeroed(key_size + value_size + 4);
        self.io_manager
            .read(&mut kv_buf, offset + actual_header_size as u64)?;

        // 构造 LogRecord
        let log_record = LogRecord {
            key: kv_buf.get(..key_size).unwrap().to_vec(),
            value: kv_buf.get(key_size..kv_buf.len() - 4).unwrap().to_vec(),
            rec_type: LogRecordType::from_u8(rec_type),
        };

        // 将 kv_buf 的读取指针向前移动到 crc 字段的位置
        kv_buf.advance(key_size + value_size);

        if kv_buf.get_u32() != log_record.get_crc() {
            return Err(Errors::InvaildLogRecordCrc);
        }

        // 构造结果并返回
        Ok(ReadLogRecord {
            record: log_record,
            size: actual_header_size + key_size + value_size + 4,
        })
    }

    /// 写 hint 索引到文件当中
    pub fn write_hint_record(&self, key: Vec<u8>, pos: LogRecordPos) -> Result<()> {
        let hint_record = LogRecord {
            key: key,
            value: pos.encode(),
            rec_type: LogRecordType::NORMAL,
        };

        let enc_record = hint_record.encode();
        self.write(&enc_record)?;
        Ok(())
    }

    pub fn write(&self, buf: &[u8]) -> Result<usize> {
        let n_bytes = self.io_manager.write(buf)?;
        // 更新 write_off 字段
        let mut write_off = self.wirte_off.write();
        *write_off += n_bytes as u64;
        Ok(n_bytes)
    }

    pub fn sync(&self) -> Result<()> {
        self.io_manager.sync()
    }

    pub fn set_io_manager(&mut self, dir_path: PathBuf, io_type: IOType) {
        self.io_manager = new_io_manager(get_data_file_name(dir_path, self.get_file_id()), io_type);
    }

    pub fn file_size(&self) -> u64 {
        self.io_manager.size()
    }
}

pub fn get_data_file_name(path: PathBuf, file_id: u64) -> PathBuf {
    let name = std::format!("{:09}", file_id) + DATA_FILE_NAME_SUFFIX;
    path.join(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_data_file() {
        let dir_path = std::env::temp_dir();

        let data_file_res1 = DataFile::new(dir_path.clone(), 0, IOType::StandardFIO);
        assert!(data_file_res1.is_ok());
        let data_file1 = data_file_res1.unwrap();
        assert_eq!(data_file1.get_file_id(), 0);

        let data_file_res2 = DataFile::new(dir_path.clone(), 0, IOType::StandardFIO);
        assert!(data_file_res2.is_ok());
        let data_file2 = data_file_res2.unwrap();
        assert_eq!(data_file2.get_file_id(), 0);

        let data_file_res3 = DataFile::new(dir_path.clone(), 660, IOType::StandardFIO);
        assert!(data_file_res3.is_ok());
        let data_file3 = data_file_res3.unwrap();
        assert_eq!(data_file3.get_file_id(), 660);
    }

    #[test]
    fn test_data_file_write() {
        let dir_path = std::env::temp_dir();
        let data_file_res1 = DataFile::new(dir_path.clone(), 100, IOType::StandardFIO);
        assert!(data_file_res1.is_ok());
        let data_file1 = data_file_res1.unwrap();
        assert_eq!(data_file1.get_file_id(), 100);

        let write_res1 = data_file1.write("aaa".as_bytes());
        assert!(write_res1.is_ok());
        assert_eq!(write_res1.unwrap(), 3 as usize);

        let write_res2 = data_file1.write("bbbb".as_bytes());
        assert!(write_res2.is_ok());
        assert_eq!(write_res2.unwrap(), 4 as usize);

        let write_res3 = data_file1.write("ccccc".as_bytes());
        assert!(write_res3.is_ok());
        assert_eq!(write_res3.unwrap(), 5 as usize);
    }

    #[test]
    fn test_data_file_sync() {
        let dir_path = std::env::temp_dir();
        let data_file_res1 = DataFile::new(dir_path.clone(), 200, IOType::StandardFIO);
        assert!(data_file_res1.is_ok());
        let data_file1 = data_file_res1.unwrap();
        assert_eq!(data_file1.get_file_id(), 200);

        let sync_res = data_file1.sync();
        assert!(sync_res.is_ok());
    }

    #[test]
    fn test_data_file_read_log_record() {
        let dir_path = std::env::temp_dir();
        let data_file_res1 = DataFile::new(dir_path.clone(), 200, IOType::StandardFIO);
        assert!(data_file_res1.is_ok());
        let data_file1 = data_file_res1.unwrap();
        assert_eq!(data_file1.get_file_id(), 200);

        // 正常的一条 LogRecord 编码
        let rec1 = LogRecord {
            key: "name".as_bytes().to_vec(),
            value: "bitcask-rs-kv".as_bytes().to_vec(),
            rec_type: LogRecordType::NORMAL,
        };
        let write_res1 = data_file1.write(&rec1.encode());
        assert!(write_res1.is_ok());

        // 从起始位置开始读取
        let read_res1 = data_file1.read_log_record(0);
        assert!(read_res1.is_ok());
        let read_enc1 = read_res1.ok().unwrap().record;
        assert_eq!(rec1.key, read_enc1.key);
        assert_eq!(rec1.value, read_enc1.value);
        assert_eq!(rec1.rec_type, read_enc1.rec_type);

        let rec2 = LogRecord {
            key: "name".as_bytes().to_vec(),
            value: "new-value".as_bytes().to_vec(),
            rec_type: LogRecordType::NORMAL,
        };
        let write_res2 = data_file1.write(&rec2.encode());
        assert!(write_res2.is_ok());

        // 从新的位置开始读取
        let read_res2 = data_file1.read_log_record(24);
        assert!(read_res2.is_ok());
        let read_enc2 = read_res2.ok().unwrap().record;
        assert_eq!(rec2.key, read_enc2.key);
        assert_eq!(rec2.value, read_enc2.value);
        assert_eq!(rec2.rec_type, read_enc2.rec_type);

        // 删除类型
        let rec3 = LogRecord {
            key: "name".as_bytes().to_vec(),
            value: Default::default(),
            rec_type: LogRecordType::DELETE,
        };
        let write_res3 = data_file1.write(&rec3.encode());
        assert!(write_res3.is_ok());

        let read_res3 = data_file1.read_log_record(44);
        assert!(read_res3.is_ok());
        let read_enc3 = read_res3.ok().unwrap().record;
        assert_eq!(rec3.key, read_enc3.key);
        assert_eq!(rec3.value, read_enc3.value);
        assert_eq!(rec3.rec_type, read_enc3.rec_type);
    }
}
