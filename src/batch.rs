use std::{
    collections::HashMap,
    sync::{atomic::Ordering, Arc},
};

use bytes::{BufMut, Bytes, BytesMut};
use parking_lot::Mutex;
use prost::{decode_length_delimiter, encode_length_delimiter};

use crate::{
    data::log_record::{LogRecord, LogRecordPos, LogRecordType},
    db::Engine,
    errors::{Errors, Result},
    options::{IndexType, WriteBatchOptions},
};

const TXN_FIN_KEY: &[u8] = "txn-fin".as_bytes();
pub(crate) const NON_TRANSACTION_SEQ_NO: usize = 0;

/// 批量写操纵，保证原子性
pub struct WriteBatch<'a> {
    pending_writes: Arc<Mutex<HashMap<Vec<u8>, LogRecord>>>,
    engine: &'a Engine,
    options: WriteBatchOptions,
}

impl Engine {
    pub fn new_write_batch(&self, options: WriteBatchOptions) -> Result<WriteBatch> {
        if self.options.index_type == IndexType::BPTree && !self.seq_file_exists && !self.is_initial
        {
            return Err(Errors::UableToUseWriteBatch);
        }

        Ok(WriteBatch {
            pending_writes: Arc::new(Mutex::new(HashMap::new())),
            engine: self,
            options: options,
        })
    }
}

impl WriteBatch<'_> {
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        // 暂存数据
        let record = LogRecord {
            key: key.to_vec(),
            value: value.to_vec(),
            rec_type: LogRecordType::NORMAL,
        };

        let mut pending_writes = self.pending_writes.lock();
        pending_writes.insert(key.to_vec(), record);

        Ok(())
    }

    pub fn delete(&self, key: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        let mut pending_writes = self.pending_writes.lock();
        // 数据不存在就直接返回
        let index_pos = self.engine.index.get(key.to_vec());
        if index_pos.is_none() {
            if pending_writes.contains_key(&key.to_vec()) {
                pending_writes.remove(&key.to_vec());
            }
            return Ok(());
        }

        // 暂存数据
        let record = LogRecord {
            key: key.to_vec(),
            value: Default::default(),
            rec_type: LogRecordType::DELETE,
        };

        pending_writes.insert(key.to_vec(), record);
        Ok(())
    }

    /// 提交数据，将数据写到数据文件中，并且更新索引
    pub fn commit(&self) -> Result<()> {
        let mut pending_write = self.pending_writes.lock();
        if pending_write.is_empty() {
            return Ok(());
        }
        if pending_write.len() > self.options.max_batch_num {
            return Err(Errors::ExceedMaxBatchNum);
        }

        // 加锁保证事务串行化
        let _lock = self.engine.batch_commit_lock.lock();

        // 获取全局事务序列号
        let seq_no = self.engine.seq_no.fetch_add(1, Ordering::SeqCst);

        let mut positions: HashMap<Vec<u8>, LogRecordPos> = HashMap::new();
        // 开始写数据到对应的数据文件
        for (_, item) in pending_write.iter() {
            let mut record = LogRecord {
                key: log_record_key_with_seq(item.key.to_vec(), seq_no),
                value: item.value.to_vec(),
                rec_type: item.rec_type,
            };

            let pos = self.engine.append_log_record(&mut record)?;
            positions.insert(item.key.to_vec(), pos);
        }

        // 写最后一条标识事务完成的数据
        let mut finish_record = LogRecord {
            key: log_record_key_with_seq(TXN_FIN_KEY.to_vec(), seq_no),
            value: Default::default(),
            rec_type: LogRecordType::TxnFinished,
        };

        self.engine.append_log_record(&mut finish_record)?;

        // 如果配置了持久化，进行持久化操作
        if self.options.sync_writes {
            let _ = self.engine.sync();
        }

        // 数据全部写完之后更新内存索引
        for (_, item) in pending_write.iter() {
            let record_pos = positions.get(&item.key).unwrap();
            if item.rec_type == LogRecordType::NORMAL {
                if let Some(old_pos) = self.engine.index.put(item.key.clone(), *record_pos) {
                    self.engine
                        .reclaim_size
                        .fetch_add(old_pos.size as usize, Ordering::SeqCst);
                }
            }

            if item.rec_type == LogRecordType::DELETE {
                let mut size = record_pos.size;

                if let Some(old_pos) = self.engine.index.delete(item.key.clone()) {
                    size += old_pos.size;
                }

                self.engine
                    .reclaim_size
                    .fetch_add(size as usize, Ordering::SeqCst);
            }
        }

        // 将暂存的数据清空
        pending_write.clear();

        Ok(())
    }
}

/// 编码 seq_no 和 key
pub(crate) fn log_record_key_with_seq(key: Vec<u8>, seq_no: usize) -> Vec<u8> {
    let mut enc_key = BytesMut::new();
    encode_length_delimiter(seq_no, &mut enc_key).unwrap();
    enc_key.extend_from_slice(&key.to_vec());
    enc_key.to_vec()
}

/// 解析 LogRecord 的 key，拿到实际的 key 和 seq no
pub(crate) fn parse_log_record_key(key: Vec<u8>) -> (Vec<u8>, usize) {
    let mut buf = BytesMut::new();
    buf.put_slice(&key);
    let seq_no = decode_length_delimiter(&mut buf).unwrap();
    (buf.to_vec(), seq_no)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::{
        options::Options,
        util::rand_kv::{get_test_key, get_test_value},
    };

    use super::*;

    #[test]
    fn test_write_batch_1() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-write-batch-1");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let wb = engine
            .new_write_batch(WriteBatchOptions::default())
            .expect("failed to create wirte batch");

        // 写数据后未提交
        let put_res1 = wb.put(get_test_key(1), get_test_value(1));
        assert!(put_res1.is_ok());
        let put_res2 = wb.put(get_test_key(2), get_test_value(2));
        assert!(put_res2.is_ok());

        let res1 = engine.get(get_test_key(1));
        assert!(res1.is_err());
        assert_eq!(res1.err().unwrap(), Errors::KeyNotFound);

        // 事务提交之后进行查询
        let commit_res1 = wb.commit();
        assert!(commit_res1.is_ok());

        let res2 = engine.get(get_test_key(1));
        assert!(res2.is_ok());
        assert_eq!(res2.unwrap(), get_test_value(1));

        // 验证事务序列号
        let seq_no = wb.engine.seq_no.load(Ordering::SeqCst);
        assert_eq!(seq_no, 2);

        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_write_batch_2() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-write-batch-2");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let wb = engine
            .new_write_batch(WriteBatchOptions::default())
            .expect("failed to create wirte batch");

        // 写数据后未提交
        let put_res1 = wb.put(get_test_key(1), get_test_value(1));
        assert!(put_res1.is_ok());
        let put_res2 = wb.put(get_test_key(2), get_test_value(2));
        assert!(put_res2.is_ok());
        let commit_res1 = wb.commit();
        assert!(commit_res1.is_ok());

        // 重启之后进行校验
        engine.close().expect("failed to close");
        let new_engine = Engine::open(opts.clone()).expect("failed to open engine");
        let keys = new_engine.list_keys();
        print!("{:#?}", keys);

        // 验证事务序列号
        let seq_no = new_engine.seq_no.load(Ordering::SeqCst);
        assert_eq!(seq_no, 2);

        let new_wb = new_engine
            .new_write_batch(WriteBatchOptions::default())
            .expect("failed to create wirte batch");
        let put_res3 = new_wb.put(get_test_key(1), get_test_value(1));
        assert!(put_res3.is_ok());
        let put_res4 = new_wb.put(get_test_key(2), get_test_value(2));
        assert!(put_res4.is_ok());
        let commit_res2 = new_wb.commit();
        assert!(commit_res2.is_ok());

        // 验证事务序列号
        let seq_no_2 = new_engine.seq_no.load(Ordering::SeqCst);
        assert_eq!(seq_no_2, 3);

        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }

    // #[test]
    // fn test_write_batch_3() {
    //     let mut opts = Options::default();
    //     opts.dir_path = PathBuf::from("/tmp/bitcask-rs-write-batch-3");
    //     opts.data_file_size = 64 * 1024 * 1024;
    //     let engine = Engine::open(opts.clone()).expect("failed to open engine");

    //     let keys = engine.list_keys();
    //     println!("keys: {:?}", keys);

    //     let mut wb_opts = WriteBatchOptions::default();
    //     wb_opts.max_batch_num = 10000000;

    //     let wb = engine
    //         .new_write_batch(wb_opts)
    //         .expect("failed to create wirte batch");

    //     for i in 0..=1000000 {
    //       let res = wb.put(get_test_key(i), get_test_value(i));
    //       assert!(res.is_ok());
    //     }

    //     // 提交时候中断程序，commit没有写入最后一个事务终止标识，所以重启启之后，是读取不到数据的
    //     let _ = wb.commit();

    //     // std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    // }
}
