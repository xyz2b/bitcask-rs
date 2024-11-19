use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use crate::errors::Result;
use crate::{db::Engine, errors::Errors, options::IteratorOptions};

use bytes::Bytes;
use lazy_static::lazy_static;
use log::error;
use parking_lot::RwLock;

use serde::{Deserialize, Serialize};

/// MVCC 事务
pub struct Transaction<'a> {
    /// 底层 KV 存储引擎
    engine: &'a Engine, // engine的引用必须比Iterator寿命长
    /// 事务版本号
    version: u64,
    /// 事务开启时的活跃事务列表，不包含自己
    active_xid: HashSet<u64>,
}

impl Engine {
    pub fn begin(&self) -> Transaction {
        Transaction::begin(self)
    }
}

impl Transaction<'_> {
    pub fn begin<'a>(engine: &'a Engine) -> Transaction {
        // 获取全局事务号
        let version = acquire_next_version();

        let mut active_txn = ACTIVE_TXN.write();
        // 这个 map 中的 key 就是当前所有的活跃事务
        let active_xid = active_txn.keys().cloned().collect();

        // 添加到当前活跃事务 id 列表中
        active_txn.insert(version, vec![]);

        // 返回结果
        Transaction {
            engine: engine,
            version: version,
            active_xid: active_xid,
        }
    }

    /// 写入数据
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }
        if value.is_empty() {
            return Err(Errors::ValueIsEmpty);
        }

        let txn_key = match self.txn_write(key) {
            Ok(key) => key,
            Err(e) => {
                return Err(e);
            }
        };

        self.engine.put(Bytes::from(txn_key.encode()), value)
    }

    /// 删除数据
    /// put 一条 value 为空的数据，此限制了用户不能 put value 为空的数据
    pub fn delete(&self, key: Bytes) -> Result<()> {
        let txn_key = match self.txn_write(key) {
            Ok(key) => key,
            Err(e) => {
                return Err(e);
            }
        };

        self.engine
            .put(Bytes::from(txn_key.encode()), Bytes::default())
    }

    fn txn_write(&self, key: Bytes) -> Result<Key> {
        // 判断当前写入的 key 是否和其他的事务冲突
        // key 是按照 key-version 排序的，所以只需要判断最近的一个 key 即可
        let engine = self.engine;
        let mut iter_opts = IteratorOptions::default();
        iter_opts.reverse = true;
        let mut iter = engine.iter(iter_opts);
        while let Some((enc_key, _)) = iter.next() {
            let key_version = decode_key(&enc_key.to_vec());
            if key_version.raw_key.eq(&key.to_vec()) {
                if !self.is_visible(key_version.version) {
                    // 有一种情况是可以写入的
                    // T1开启事务，写入了key1，还未提交。之后T2开启事务，此时T2是不能写入key1的，但是如果此时T1提交，T2是可以写入key1的，
                    // 所以需要在这里判断下T1是否提交，已提交的事务版本号会从 ACTIVE_TXN 中删除，直接判断在不在其中即可
                    let active_txn = ACTIVE_TXN.read();
                    if !active_txn.contains_key(&key_version.version) {
                        break;
                    }
                    return Err(Errors::MvccTxnWriteKeyConflictsWithOtherTransactions);
                }
                break;
            }
        }

        // 写入 TxnWrite
        let mut active_txn = ACTIVE_TXN.write();
        active_txn
            .entry(self.version)
            .and_modify(|keys| keys.push(key.to_vec()))
            .or_insert_with(|| vec![key.to_vec()]);

        // 写入数据
        let enc_key = Key {
            raw_key: key.to_vec(),
            version: self.version,
        };

        Ok(enc_key)
    }

    /// 读取数据，从最后一条数据进行遍历，找到第一条可见的数据
    pub fn get(&self, key: Bytes) -> Result<Bytes> {
        let engine = self.engine;
        let mut iter_opts = IteratorOptions::default();
        iter_opts.reverse = true;
        let mut iter = engine.iter(iter_opts);
        while let Some((enc_key, v)) = iter.next() {
            let key_version = decode_key(&enc_key.to_vec());
            if key_version.raw_key.eq(&key.to_vec()) {
                if self.is_visible(key_version.version) {
                    if v.is_empty() {
                        return Err(Errors::KeyNotFound);
                    }
                    return Ok(v);
                }
            }
        }

        return Err(Errors::KeyNotFound);
    }

    /// 提交事务
    pub fn commit(&self) -> Result<()> {
        // 清除活跃列表中的数据
        let mut active_txn = ACTIVE_TXN.write();
        match active_txn.remove(&self.version) {
            Some(_) => {
                return Ok(());
            }
            None => {
                return Err(Errors::MvccCommitActiveTxnIsNotExist);
            }
        }
    }

    /// 回滚事务
    pub fn rollback(&self) -> Result<()> {
        // 清除写入的数据
        let mut active_txn = ACTIVE_TXN.write();
        if let Some(keys) = active_txn.get(&self.version) {
            let engine = self.engine;
            for k in keys {
                let enc_key = Key {
                    raw_key: k.to_vec(),
                    version: self.version,
                };
                match engine.delete(Bytes::from(enc_key.encode())) {
                    Ok(_) => {}
                    Err(e) => {
                        error!("engine delete data failed, {}", e);
                        // 清除活跃事务列表中的数据
                        active_txn.remove(&self.version);
                        return Err(Errors::MvccRollbackDeleteDataFailed);
                    }
                }
            }
        }

        // 清除活跃事务列表中的数据
        active_txn.remove(&self.version);
        Ok(())
    }

    // 判断一个版本的数据对当前事务是否可见
    // 1. 如果是另一个活跃事务，则不可见
    // 2. 如果版本号比当前大，则不可见
    fn is_visible(&self, version: u64) -> bool {
        if self.active_xid.contains(&version) {
            return false;
        }
        version <= self.version
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Key {
    raw_key: Vec<u8>,
    version: u64,
}

impl Key {
    fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }
}

fn decode_key(b: &Vec<u8>) -> Key {
    bincode::deserialize(&b).unwrap()
}

/// 全局递增的版本号
static VERSION: AtomicU64 = AtomicU64::new(1);

/// 获取下一个版本号
fn acquire_next_version() -> u64 {
    let version = VERSION.fetch_add(1, Ordering::SeqCst);
    version
}

lazy_static! {
  /// 当前活跃事务，包含当前活跃事务ID以及已经写入的key信息
  static ref ACTIVE_TXN: Arc<RwLock<HashMap<u64, Vec<Vec<u8>>>>> = Arc::new(RwLock::new(HashMap::new()));
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::options::Options;

    #[test]
    fn test_mvcc_put() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-mvcc-put");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        // 单事务读写
        let txn1 = engine.begin();
        let put_txn1_res1 = txn1.put(Bytes::from("key1"), Bytes::from("1"));
        assert!(put_txn1_res1.is_ok());
        let get_txn1_res1 = txn1.get(Bytes::from("key1"));
        assert!(get_txn1_res1.is_ok());
        assert_eq!(get_txn1_res1.unwrap(), Bytes::from("1"));
        let commit_txn1_res1 = txn1.commit();
        assert!(commit_txn1_res1.is_ok());

        let txn2 = engine.begin();
        // 新的事物读取已提交事务写入的数据
        let get_txn2_res1 = txn2.get(Bytes::from("key1"));
        assert!(get_txn2_res1.is_ok());
        assert_eq!(get_txn2_res1.unwrap(), Bytes::from("1"));
        // 更新已提交事务写入的数据
        let put_txn2_res1 = txn2.put(Bytes::from("key1"), Bytes::from("2"));
        assert!(put_txn2_res1.is_ok());
        let get_txn2_res2 = txn2.get(Bytes::from("key1"));
        assert!(get_txn2_res2.is_ok());
        assert_eq!(get_txn2_res2.unwrap(), Bytes::from("2"));

        let txn3 = engine.begin();
        // 读不到未提交事务写入的数据
        let get_txn3_res1 = txn3.get(Bytes::from("key1"));
        assert!(get_txn3_res1.is_ok());
        assert_eq!(get_txn3_res1.unwrap(), Bytes::from("1"));

        // 两个事物未提交前同时写一个key
        let put_txn3_res1 = txn3.put(Bytes::from("key1"), Bytes::from("3"));
        assert!(put_txn3_res1.is_err());
        assert_eq!(
            put_txn3_res1.err().unwrap(),
            Errors::MvccTxnWriteKeyConflictsWithOtherTransactions
        );

        // 未提交事务读不到其事务期间事务提交的写入数据（因为t3事务开启时，t2还没提交，可重复读）
        let commit_txn2_res1 = txn2.commit();
        assert!(commit_txn2_res1.is_ok());
        let get_txn3_res2 = txn3.get(Bytes::from("key1"));
        assert!(get_txn3_res2.is_ok());
        assert_eq!(get_txn3_res2.unwrap(), Bytes::from("1"));

        // 继续更新已提交事务的 key
        let put_txn3_res2 = txn3.put(Bytes::from("key1"), Bytes::from("3"));
        assert!(put_txn3_res2.is_ok());
        let get_txn3_res3 = txn3.get(Bytes::from("key1"));
        assert!(get_txn3_res3.is_ok());
        assert_eq!(get_txn3_res3.unwrap(), Bytes::from("3"));

        let commit_txn3_res1 = txn3.commit();
        assert!(commit_txn3_res1.is_ok());

        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_mvcc_delete() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-mvcc-delete");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let txn1 = engine.begin();
        let put_txn1_res1 = txn1.put(Bytes::from("key1"), Bytes::from("key11"));
        assert!(put_txn1_res1.is_ok());
        let put_txn1_res2 = txn1.put(Bytes::from("key2"), Bytes::from("key21"));
        assert!(put_txn1_res2.is_ok());
        let put_txn1_res3 = txn1.put(Bytes::from("key3"), Bytes::from("key31"));
        assert!(put_txn1_res3.is_ok());
        let commit_txn1_res1 = txn1.commit();
        assert!(commit_txn1_res1.is_ok());

        let txn2 = engine.begin();
        let get_txn2_res1 = txn2.get(Bytes::from("key1"));
        assert!(get_txn2_res1.is_ok());
        assert_eq!(get_txn2_res1.unwrap(), Bytes::from("key11"));

        let delete_txn2_res1 = txn2.delete(Bytes::from("key1"));
        assert!(delete_txn2_res1.is_ok());

        let get_txn2_res2 = txn2.get(Bytes::from("key1"));
        assert!(get_txn2_res2.is_err());
        assert_eq!(get_txn2_res2.err().unwrap(), Errors::KeyNotFound);

        let get_txn2_res3 = txn2.get(Bytes::from("key2"));
        assert!(get_txn2_res3.is_ok());
        assert_eq!(get_txn2_res3.unwrap(), Bytes::from("key21"));

        let txn3 = engine.begin();
        let get_txn3_res1 = txn3.get(Bytes::from("key1"));
        assert!(get_txn3_res1.is_ok());
        assert_eq!(get_txn3_res1.unwrap(), Bytes::from("key11"));

        let commit_txn2_res1 = txn2.commit();
        assert!(commit_txn2_res1.is_ok());

        let get_txn3_res2 = txn3.get(Bytes::from("key1"));
        assert!(get_txn3_res2.is_ok());
        assert_eq!(get_txn3_res2.unwrap(), Bytes::from("key11"));

        let commit_txn3_res1 = txn3.commit();
        assert!(commit_txn3_res1.is_ok());

        let txn4 = engine.begin();
        let get_txn4_res1 = txn4.get(Bytes::from("key1"));
        assert!(get_txn4_res1.is_err());
        assert_eq!(get_txn4_res1.err().unwrap(), Errors::KeyNotFound);

        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_mvcc_rollback() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-mvcc-rollback");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let txn1 = engine.begin();
        let put_txn1_res1 = txn1.put(Bytes::from("key1"), Bytes::from("key11"));
        assert!(put_txn1_res1.is_ok());
        let put_txn1_res2 = txn1.put(Bytes::from("key2"), Bytes::from("key21"));
        assert!(put_txn1_res2.is_ok());
        let put_txn1_res3 = txn1.put(Bytes::from("key3"), Bytes::from("key31"));
        assert!(put_txn1_res3.is_ok());
        let commit_txn1_res1 = txn1.commit();
        assert!(commit_txn1_res1.is_ok());

        let txn2 = engine.begin();
        let get_txn2_res1 = txn2.get(Bytes::from("key1"));
        assert!(get_txn2_res1.is_ok());
        assert_eq!(get_txn2_res1.unwrap(), Bytes::from("key11"));

        let delete_txn2_res1 = txn2.delete(Bytes::from("key1"));
        assert!(delete_txn2_res1.is_ok());

        let put_txn2_res1 = txn2.put(Bytes::from("key2"), Bytes::from("key22"));
        assert!(put_txn2_res1.is_ok());

        let get_txn2_res2 = txn2.get(Bytes::from("key1"));
        assert!(get_txn2_res2.is_err());
        assert_eq!(get_txn2_res2.err().unwrap(), Errors::KeyNotFound);

        let get_txn2_res3 = txn2.get(Bytes::from("key2"));
        assert!(get_txn2_res3.is_ok());
        assert_eq!(get_txn2_res3.unwrap(), Bytes::from("key22"));

        let rollback_txn2_res1 = txn2.rollback();
        assert!(rollback_txn2_res1.is_ok());

        let get_txn2_res4 = txn2.get(Bytes::from("key1"));
        assert!(get_txn2_res4.is_ok());
        assert_eq!(get_txn2_res4.unwrap(), Bytes::from("key11"));

        let txn3 = engine.begin();
        let get_txn3_res1 = txn3.get(Bytes::from("key1"));
        assert!(get_txn3_res1.is_ok());
        assert_eq!(get_txn3_res1.unwrap(), Bytes::from("key11"));

        let commit_txn2_res1 = txn2.commit();
        assert!(commit_txn2_res1.is_err());
        assert_eq!(
            commit_txn2_res1.err().unwrap(),
            Errors::MvccCommitActiveTxnIsNotExist
        );

        let get_txn3_res2 = txn3.get(Bytes::from("key1"));
        assert!(get_txn3_res2.is_ok());
        assert_eq!(get_txn3_res2.unwrap(), Bytes::from("key11"));

        let get_txn3_res3 = txn3.get(Bytes::from("key2"));
        assert!(get_txn3_res3.is_ok());
        assert_eq!(get_txn3_res3.unwrap(), Bytes::from("key21"));

        let commit_txn3_res1 = txn3.commit();
        assert!(commit_txn3_res1.is_ok());

        let txn4 = engine.begin();
        let get_txn4_res1 = txn4.get(Bytes::from("key1"));
        assert!(get_txn4_res1.is_ok());
        assert_eq!(get_txn4_res1.unwrap(), Bytes::from("key11"));

        let get_txn4_res2 = txn4.get(Bytes::from("key2"));
        assert!(get_txn4_res2.is_ok());
        assert_eq!(get_txn4_res2.unwrap(), Bytes::from("key21"));

        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }
}
