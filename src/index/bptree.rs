use std::{path::PathBuf, sync::Arc};

use bytes::Bytes;
use jammdb::DB;

use crate::{
    data::log_record::{decode_log_record_pos, LogRecordPos},
    options::IteratorOptions,
};

use super::{IndexIterator, Indexer};

const BPTREE_INDEXER_FILE_NAME: &str = "bptree-index";
const BPTREE_BUCKET_NAME: &str = "bitcask-index";

pub struct BPTree {
    tree: Arc<DB>,
}

impl BPTree {
    pub fn new(dir_path: PathBuf) -> Self {
        // 打开 B+ 树实例，并创建对应的 bucket
        let bptree =
            DB::open(dir_path.join(BPTREE_INDEXER_FILE_NAME)).expect("failed to open bptree");
        let tree = Arc::new(bptree);
        let tx = tree.tx(true).expect("failed to begin tx");
        tx.get_or_create_bucket(BPTREE_BUCKET_NAME).unwrap();
        tx.commit().unwrap();

        Self { tree: tree }
    }
}

impl Indexer for BPTree {
    fn put(&self, key: Vec<u8>, pos: crate::data::log_record::LogRecordPos) -> Option<LogRecordPos> {
        let mut result = None;
        let tx = self.tree.tx(true).expect("failed to begin tx");
        let bucket = tx.get_bucket(BPTREE_BUCKET_NAME).unwrap();

        if let Some(kv) = bucket.get_kv(key.clone()) {
            result = Some(decode_log_record_pos(kv.value().to_vec()));
        }

        bucket.put(key, pos.encode()).expect("failed to put value");
        tx.commit().unwrap();
        result
    }

    fn get(&self, key: Vec<u8>) -> Option<crate::data::log_record::LogRecordPos> {
        let tx = self.tree.tx(false).expect("failed to begin tx");
        let bucket = tx.get_bucket(BPTREE_BUCKET_NAME).unwrap();
        if let Some(kv) = bucket.get_kv(key) {
            return Some(decode_log_record_pos(kv.value().to_vec()));
        }
        None
    }

    fn delete(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        let mut result = None;
        let tx = self.tree.tx(true).expect("failed to begin tx");
        let bucket = tx.get_bucket(BPTREE_BUCKET_NAME).unwrap();
        if let Ok(kv) = bucket.delete(key) {
            result = Some(decode_log_record_pos(kv.value().to_vec()));
        };

        tx.commit().unwrap();
        result
    }

    fn list_keys(&self) -> crate::errors::Result<Vec<bytes::Bytes>> {
        let tx = self.tree.tx(false).expect("failed to begin tx");
        let bucket = tx.get_bucket(BPTREE_BUCKET_NAME).unwrap();
        let mut keys = Vec::new();

        for data in bucket.cursor() {
            keys.push(Bytes::copy_from_slice(data.key()));
        }

        Ok(keys)
    }

    fn iterator(&self, options: crate::options::IteratorOptions) -> Box<dyn super::IndexIterator> {
        let tx = self.tree.tx(false).expect("failed to begin tx");
        let bucket = tx.get_bucket(BPTREE_BUCKET_NAME).unwrap();

        let mut items = Vec::new();
        // 将 BTree 中的数据存储到数组中
        for data in bucket.cursor() {
            items.push((
                data.key().to_vec(),
                decode_log_record_pos(data.kv().value().to_vec()),
            ));
        }

        if options.reverse {
            items.reverse();
        }

        Box::new(BPTreeIterator {
            items,
            curr_index: 0,
            options,
        })
    }

    fn clear(&self) {
        let tx = self.tree.tx(true).expect("failed to begin tx");
        tx.delete_bucket(BPTREE_BUCKET_NAME).unwrap();
        tx.get_or_create_bucket(BPTREE_BUCKET_NAME).unwrap();
        tx.commit().unwrap();
    }
}

pub struct BPTreeIterator {
    items: Vec<(Vec<u8>, LogRecordPos)>, // 存储 key+索引，根据 key 进行排序过的
    curr_index: usize,                   // 当前遍历的下标
    options: IteratorOptions,            // 配置项
}

impl IndexIterator for BPTreeIterator {
    fn rewind(&mut self) {
        self.curr_index = 0;
    }

    fn seek(&mut self, key: Vec<u8>) {
        // 二分查找
        self.curr_index = match self.items.binary_search_by(|(x, _)| {
            if self.options.reverse {
                x.cmp(&key).reverse()
            } else {
                x.cmp(&key)
            }
        }) {
            Ok(equal_value) => equal_value,
            Err(insert_val) => insert_val,
        };
    }

    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordPos)> {
        if self.curr_index >= self.items.len() {
            return None;
        }

        while let Some(item) = self.items.get(self.curr_index) {
            self.curr_index += 1;
            let prefix = &self.options.prefix;
            if prefix.is_empty() || item.0.starts_with(&prefix) {
                return Some((&item.0, &item.1));
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    #[test]
    fn test_bptree_put() {
        let dir_path = PathBuf::from("/tmp/bitcask-rs-bptree-put");
        fs::create_dir_all(dir_path.clone()).unwrap();
        let bpt = BPTree::new(dir_path.clone());

        let res1 = bpt.put(
            "".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
                size: 11,
            },
        );
        assert!(res1.is_none());
        let res2 = bpt.put(
            "aa".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
                size: 11,
            },
        );
        assert!(res2.is_none());

        let res3 = bpt.put("aa".as_bytes().to_vec(), LogRecordPos {file_id: 1, offset: 100, size: 11});
        assert!(res3.is_some());

        std::fs::remove_dir_all(dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_bptree_get() {
        let dir_path = PathBuf::from("/tmp/bitcask-rs-bptree-get");
        fs::create_dir_all(dir_path.clone()).unwrap();
        let bpt = BPTree::new(dir_path.clone());

        let res1 = bpt.put(
            "".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
                size: 11,
            },
        );
        assert!(res1.is_none());
        let res2 = bpt.put(
            "aa".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 11,
                offset: 22,
                size: 11,
            },
        );
        assert!(res2.is_none());

        let pos1 = bpt.get("".as_bytes().to_vec());
        assert!(pos1.is_some());
        assert_eq!(pos1.unwrap().file_id, 1);
        assert_eq!(pos1.unwrap().offset, 10);
        let pos2 = bpt.get("aa".as_bytes().to_vec());
        assert!(pos2.is_some());
        assert_eq!(pos2.unwrap().file_id, 11);
        assert_eq!(pos2.unwrap().offset, 22);

        std::fs::remove_dir_all(dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_bptree_delete() {
        let dir_path = PathBuf::from("/tmp/bitcask-rs-bptree-delete");
        fs::create_dir_all(dir_path.clone()).unwrap();
        let bpt = BPTree::new(dir_path.clone());

        let res1 = bpt.put(
            "".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
                size: 11,
            },
        );
        assert!(res1.is_none());
        let res2 = bpt.put(
            "aa".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 11,
                offset: 22,
                size: 11,
            },
        );
        assert!(res2.is_none());

        let del1 = bpt.delete("".as_bytes().to_vec());
        assert!(del1.is_some());
        let del2 = bpt.delete("aa".as_bytes().to_vec());
        assert!(del2.is_some());
        let del3 = bpt.delete("not_exist".as_bytes().to_vec());
        assert!(del3.is_none());

        std::fs::remove_dir_all(dir_path).expect("failed to remove path");
    }



    #[test]
    fn test_bptree_clear() {
        let dir_path = PathBuf::from("/tmp/bitcask-rs-bptree-clear");
        fs::create_dir_all(dir_path.clone()).unwrap();
        let bpt = BPTree::new(dir_path.clone());

        let res1 = bpt.put(
            "".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
                size: 11,
            },
        );
        assert!(res1.is_none());
        let res2 = bpt.put(
            "aa".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 11,
                offset: 22,
                size: 11,
            },
        );
        assert!(res2.is_none());

        let pos1 = bpt.get("".as_bytes().to_vec());
        assert!(pos1.is_some());
        assert_eq!(pos1.unwrap().file_id, 1);
        assert_eq!(pos1.unwrap().offset, 10);

        bpt.clear();

        let pos2 = bpt.get("".as_bytes().to_vec());
        assert!(pos2.is_none());

        std::fs::remove_dir_all(dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_bptree_iterator_seek() {
        let dir_path = PathBuf::from("/tmp/bitcask-rs-bptree-iter-seek");
        fs::create_dir_all(dir_path.clone()).unwrap();
        let bpt = BPTree::new(dir_path.clone());

        // 没有数据的情况
        let mut iter1 = bpt.iterator(IteratorOptions::default());
        iter1.seek("as".as_bytes().to_vec());
        let res1 = iter1.next();
        assert!(res1.is_none());

        // 有一条数据的情况
        bpt.put(
            "ccde".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
                size: 11,
            },
        );
        let mut iter2 = bpt.iterator(IteratorOptions::default());
        iter2.seek("aa".as_bytes().to_vec());
        let res2 = iter2.next();
        assert!(res2.is_some());

        let mut iter3 = bpt.iterator(IteratorOptions::default());
        iter3.seek("zz".as_bytes().to_vec());
        let res3 = iter3.next();
        assert!(res3.is_none());

        // 有多条数据的情况
        bpt.put(
            "bbde".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
                size: 11,
            },
        );
        bpt.put(
            "aaed".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
                size: 11,
            },
        );
        bpt.put(
            "cadd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
                size: 11,
            },
        );
        let mut iter4 = bpt.iterator(IteratorOptions::default());
        iter4.seek("b".as_bytes().to_vec());

        while let Some(item) = iter4.next() {
            assert!(item.0.len() > 0);
            // println!("{:?}", String::from_utf8(item.0.to_vec()));
        }

        let mut iter5 = bpt.iterator(IteratorOptions::default());
        iter5.seek("cadd".as_bytes().to_vec());
        while let Some(item) = iter5.next() {
            assert!(item.0.len() > 0);
            // println!("{:?}", String::from_utf8(item.0.to_vec()));
        }

        // 反向迭代
        let mut iter_opts = IteratorOptions::default();
        iter_opts.reverse = true;
        let mut iter6 = bpt.iterator(iter_opts);
        iter6.seek("bb".as_bytes().to_vec());
        while let Some(item) = iter6.next() {
            assert!(item.0.len() > 0);
            // println!("{:?}", String::from_utf8(item.0.to_vec()));
        }

        std::fs::remove_dir_all(dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_bptree_iterator_next() {
        let dir_path = PathBuf::from("/tmp/bitcask-rs-bptree-iter-next");
        fs::create_dir_all(dir_path.clone()).unwrap();
        let bpt = BPTree::new(dir_path.clone());

        // 没有数据的情况
        let mut iter1 = bpt.iterator(IteratorOptions::default());
        iter1.seek("as".as_bytes().to_vec());
        let res1 = iter1.next();
        assert!(res1.is_none());

        // 有一条数据的情况
        bpt.put(
            "ccde".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
                size: 11,
            },
        );
        let mut iter2 = bpt.iterator(IteratorOptions::default());
        iter2.seek("aa".as_bytes().to_vec());
        let res2: Option<(&Vec<u8>, &LogRecordPos)> = iter2.next();
        assert!(res2.is_some());

        // 有多条数据的情况
        bpt.put(
            "bbde".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
                size: 11,
            },
        );
        bpt.put(
            "aaed".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
                size: 11,
            },
        );
        bpt.put(
            "cadd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
                size: 11,
            },
        );
        let mut iter3 = bpt.iterator(IteratorOptions::default());
        iter3.seek("b".as_bytes().to_vec());

        while let Some(item) = iter3.next() {
            assert!(item.0.len() > 0);
            // println!("{:?}", String::from_utf8(item.0.to_vec()));
        }

        // 反向迭代
        let mut iter_opts1 = IteratorOptions::default();
        iter_opts1.reverse = true;
        let mut iter4 = bpt.iterator(iter_opts1);
        iter4.seek("dd".as_bytes().to_vec());
        let res4: Option<(&Vec<u8>, &LogRecordPos)> = iter4.next();
        assert!(res4.is_some());
        while let Some(item) = iter4.next() {
            assert!(item.0.len() > 0);
            println!("{:?}", String::from_utf8(item.0.to_vec()));
        }

        // 有前缀的情况
        let mut iter_opts2 = IteratorOptions::default();
        iter_opts2.prefix = "c".as_bytes().to_vec();
        let mut iter5 = bpt.iterator(iter_opts2);
        while let Some(item) = iter5.next() {
            assert!(item.0.len() > 0);
            // println!("{:?}", String::from_utf8(item.0.to_vec()));
        }

        let mut iter_opts3 = IteratorOptions::default();
        iter_opts3.prefix = "csss".as_bytes().to_vec();
        let mut iter6 = bpt.iterator(iter_opts3);
        let res6 = iter6.next();
        assert!(res6.is_none());

        let mut iter_opts4 = IteratorOptions::default();
        iter_opts4.prefix = "cadd".as_bytes().to_vec();
        let mut iter6 = bpt.iterator(iter_opts4);
        while let Some(item) = iter6.next() {
            assert!(item.0.len() > 0);
            // println!("{:?}", String::from_utf8(item.0.to_vec()));
        }

        std::fs::remove_dir_all(dir_path).expect("failed to remove path");
    }
}
