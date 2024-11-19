use core::panic;

use bytes::{BufMut, BytesMut};
use prost::{encode_length_delimiter, length_delimiter_len};


#[derive(Debug, PartialEq, Clone, Copy)]
pub enum LogRecordType {
    // 正常 put 的数据
    NORMAL = 1,

    // 被删除的数据标识，墓碑值
    DELETE = 2,
}

impl LogRecordType {
    pub fn from_u8(v: u8) -> Self {
      match v {
        1 => LogRecordType::NORMAL,
        2 => LogRecordType::DELETE,
        _ => panic!("unknown log record type"),
      }
    }
}

/// 写入到数据文件的记录
/// 之所以叫日志，是因为数据文件中的数据是追加写入的，类似日志的格式
#[derive(Debug)]
pub struct LogRecord {
  pub(crate) key: Vec<u8>,
  pub(crate) value: Vec<u8>,
  pub(crate) rec_type: LogRecordType,
}

impl LogRecord {
    /// 对 LogRecord 进行编码，返回字节数组及长度
    /// 
    /// +--------------+--------------+------------+--------------+--------------+--------------+
    /// |   type 类型   |   key size   | value size |    key       |  value       | crc 校验值    |
    /// +--------------+--------------+------------+--------------+--------------+--------------+
    ///      1字节           变长(最大5)   变长(最大5)   变长             变长             4字节
    /// 
    pub fn encode(&self) -> Vec<u8> {
      let (enc_buf, _) = self.encode_and_get_crc();
      enc_buf
    }

    pub fn get_crc(&self) -> u32 {
      let (_, crc) = self.encode_and_get_crc();
      crc
    }

    fn encode_and_get_crc(&self) -> (Vec<u8>, u32) {
      // 初始化字节数组，存放编码数据
      let mut buf = BytesMut::new();
      buf.reserve(self.encoded_length());

      // 第一个字节存放 Type 类型
      buf.put_u8(self.rec_type as u8);

      // 再存储 key 和 value 的长度
      encode_length_delimiter(self.key.len(), &mut buf).unwrap();
      // 用分隔符分割的长度信息
      encode_length_delimiter(self.value.len(), &mut buf).unwrap();

      // 存储 key 和  value
      buf.extend_from_slice(&self.key);
      buf.extend_from_slice(&self.value);

      // 计算 crc 并存储
      let mut hasher = crc32fast::Hasher::new();
      hasher.update(&buf);
      let crc = hasher.finalize();
      buf.put_u32(crc);
      
      (buf.to_vec(), crc)
    }

    fn encoded_length(&self) -> usize {
      std::mem::size_of::<u8>() 
      + length_delimiter_len(self.key.len()) 
      + length_delimiter_len(self.value.len())
      + self.key.len() + self.value.len() 
      + 4
    }
}


/// 数据位置索引信息，描述数据存储到了哪个位置
#[derive(Clone, Copy)]
pub struct LogRecordPos {
  pub(crate) file_id: u32,
  pub(crate) offset: u64,
}

/// 从数据文件中读取的 log_record 信息，包含其 size
#[derive(Debug)]
pub struct ReadLogRecord {
  pub(crate) record: LogRecord,
  pub(crate) size: usize,
}

/// 获取 LogRecord header 部分的最大长度
pub fn max_log_record_header_size() -> usize {
  std::mem::size_of::<u8>() + length_delimiter_len(std::mem::size_of::<u32>()) * 2
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_log_record_encode() {
    // 正常的一条 LogRecord 编码
    let rec1 = LogRecord {
      key: "name".as_bytes().to_vec(),
      value: "bitcask-rs".as_bytes().to_vec(),
      rec_type: LogRecordType::NORMAL,
    };
    let enc1 = rec1.encode();
    assert!(enc1.len() > 5);
    assert_eq!(1020360578, rec1.get_crc());

    // LogRecord 的 value 为空
    let rec2 = LogRecord {
      key: "name".as_bytes().to_vec(),
      value: Default::default(),
      rec_type: LogRecordType::NORMAL,
    };
    let enc2 = rec2.encode();
    assert!(enc2.len() > 5);
    assert_eq!(3756865478, rec2.get_crc());

    // 类型为 Deleted 的情况
    let rec3 = LogRecord {
      key: "name".as_bytes().to_vec(),
      value: "bitcask-rs".as_bytes().to_vec(),
      rec_type: LogRecordType::DELETE,
    };
    let enc3 = rec3.encode();
    assert!(enc3.len() > 5);
    assert_eq!(1867197446, rec3.get_crc());
  }
}