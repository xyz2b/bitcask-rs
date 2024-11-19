use std::{collections::BTreeMap, sync::Arc};

use bytes::Bytes;
use parking_lot::RwLock;

use crate::{data::log_record::LogRecordPos, options::IteratorOptions, errors::Result};

use super::{IndexIterator, Indexer};


/// BTree索引，主要封装了标准库中的 BTreeMap
pub struct BTree {
  tree: Arc<RwLock<BTreeMap<Vec<u8>, LogRecordPos>>>,
}

impl BTree {
  pub fn new() -> Self {
    Self {
      tree: Arc::new(RwLock::new(BTreeMap::new())),
    }
  }
}

impl Indexer for BTree {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool {
        let mut write_guard = self.tree.write();
        write_guard.insert(key, pos);
        true
    }

    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        let read_guard = self.tree.read();
        read_guard.get(&key).copied()
    }

    fn delete(&self, key: Vec<u8>) -> bool {
        let mut write_guard = self.tree.write();
        let remove_res = write_guard.remove(&key);
        remove_res.is_some()
    }
    
    fn iterator(&self, options: IteratorOptions) -> Box<dyn IndexIterator> {
        let read_guard = self.tree.read();
        let mut items = Vec::with_capacity(read_guard.len());
        // 将 BTree 中的数据存储到数组中
        for (key, value) in read_guard.iter() {
          items.push((key.clone(), value.clone()));
        }

        if options.reverse {
          items.reverse();
        }

        Box::new(
          BTreeIterator {
            items,
            curr_index: 0,
            options
          }
        )
    }
    
    fn list_keys(&self) -> Result<Vec<bytes::Bytes>> {
        let read_guard = self.tree.read();
        let mut keys = Vec::with_capacity(read_guard.len());
        for(k, _) in read_guard.iter() {
          keys.push(Bytes::copy_from_slice(&k));
        }
        Ok(keys)
    }
}

pub struct BTreeIterator {
  items: Vec<(Vec<u8>, LogRecordPos)>, // 存储 key+索引，根据 key 进行排序过的
  curr_index: usize, // 当前遍历的下标
  options: IteratorOptions, // 配置项
}

impl IndexIterator for BTreeIterator {
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
  use std::iter;

use super::*;

  #[test]
  fn test_btree_put() {
    let bt = BTree::new();

    let res1 = bt.put("".as_bytes().to_vec(), LogRecordPos {file_id: 1, offset: 10});
    assert_eq!(res1, true);
    let res2 = bt.put("aa".as_bytes().to_vec(), LogRecordPos {file_id: 1, offset: 10});
    assert_eq!(res2, true);
  }

  #[test]
  fn test_btree_get() {
    let bt = BTree::new();

    let res1 = bt.put("".as_bytes().to_vec(), LogRecordPos {file_id: 1, offset: 10});
    assert_eq!(res1, true);
    let res2 = bt.put("aa".as_bytes().to_vec(), LogRecordPos {file_id: 11, offset: 22});
    assert_eq!(res2, true);

    let pos1 = bt.get("".as_bytes().to_vec());
    assert!(pos1.is_some());
    assert_eq!(pos1.unwrap().file_id, 1);
    assert_eq!(pos1.unwrap().offset, 10);
    let pos2 = bt.get("aa".as_bytes().to_vec());
    assert!(pos2.is_some());
    assert_eq!(pos2.unwrap().file_id, 11);
    assert_eq!(pos2.unwrap().offset, 22);
  }

  #[test]
  fn test_btree_delete() {
    let bt = BTree::new();

    let res1 = bt.put("".as_bytes().to_vec(), LogRecordPos {file_id: 1, offset: 10});
    assert_eq!(res1, true);
    let res2 = bt.put("aa".as_bytes().to_vec(), LogRecordPos {file_id: 11, offset: 22});
    assert_eq!(res2, true);

    let del1 = bt.delete("".as_bytes().to_vec());
    assert!(del1);
    let del2 = bt.delete("aa".as_bytes().to_vec());
    assert!(del2);
    let del3 = bt.delete("not_exist".as_bytes().to_vec());
    assert!(!del3);
  }

  #[test]
  fn test_btree_iterator_seek() {
    let bt = BTree::new();

    // 没有数据的情况
    let mut iter1 = bt.iterator(IteratorOptions::default());
    iter1.seek("as".as_bytes().to_vec());
    let res1 = iter1.next();
    assert!(res1.is_none());

    // 有一条数据的情况
    bt.put("ccde".as_bytes().to_vec(), LogRecordPos{file_id: 1, offset: 10});
    let mut iter2 = bt.iterator(IteratorOptions::default());
    iter2.seek("aa".as_bytes().to_vec());
    let res2 = iter2.next();
    assert!(res2.is_some());

    let mut iter3 = bt.iterator(IteratorOptions::default());
    iter3.seek("zz".as_bytes().to_vec());
    let res3 = iter3.next();
    assert!(res3.is_none());

    // 有多条数据的情况
    bt.put("bbde".as_bytes().to_vec(), LogRecordPos{file_id: 1, offset: 10});
    bt.put("aaed".as_bytes().to_vec(), LogRecordPos{file_id: 1, offset: 10});
    bt.put("cadd".as_bytes().to_vec(), LogRecordPos{file_id: 1, offset: 10});
    let mut iter4 = bt.iterator(IteratorOptions::default());
    iter4.seek("b".as_bytes().to_vec());

    while let Some(item) = iter4.next() {
        assert!(item.0.len() > 0);
        // println!("{:?}", String::from_utf8(item.0.to_vec()));
    }

    let mut iter5 = bt.iterator(IteratorOptions::default());
    iter5.seek("cadd".as_bytes().to_vec());
    while let Some(item ) = iter5.next() {
        assert!(item.0.len() > 0);
        // println!("{:?}", String::from_utf8(item.0.to_vec()));
    }

    // 反向迭代
    let mut iter_opts = IteratorOptions::default();
    iter_opts.reverse = true;
    let mut iter6 = bt.iterator(iter_opts);
    iter6.seek("bb".as_bytes().to_vec());
    while let Some(item ) = iter6.next() {
      assert!(item.0.len() > 0);
      // println!("{:?}", String::from_utf8(item.0.to_vec()));
    }
  }

  #[test]
  fn test_btree_iterator_next() {
    let bt = BTree::new();

    // 没有数据的情况
    let mut iter1 = bt.iterator(IteratorOptions::default());
    iter1.seek("as".as_bytes().to_vec());
    let res1 = iter1.next();
    assert!(res1.is_none());

    // 有一条数据的情况
    bt.put("ccde".as_bytes().to_vec(), LogRecordPos{file_id: 1, offset: 10});
    let mut iter2 = bt.iterator(IteratorOptions::default());
    iter2.seek("aa".as_bytes().to_vec());
    let res2: Option<(&Vec<u8>, &LogRecordPos)> = iter2.next();
    assert!(res2.is_some());

    // 有多条数据的情况
    bt.put("bbde".as_bytes().to_vec(), LogRecordPos{file_id: 1, offset: 10});
    bt.put("aaed".as_bytes().to_vec(), LogRecordPos{file_id: 1, offset: 10});
    bt.put("cadd".as_bytes().to_vec(), LogRecordPos{file_id: 1, offset: 10});
    let mut iter3 = bt.iterator(IteratorOptions::default());
    iter3.seek("b".as_bytes().to_vec());

    while let Some(item) = iter3.next() {
      assert!(item.0.len() > 0);
      // println!("{:?}", String::from_utf8(item.0.to_vec()));
    }

    // 反向迭代
    let mut iter_opts1 = IteratorOptions::default();
    iter_opts1.reverse = true;
    let mut iter4 = bt.iterator(iter_opts1);
    iter4.seek("dd".as_bytes().to_vec());
    let res4: Option<(&Vec<u8>, &LogRecordPos)> = iter4.next();
    assert!(res4.is_some());
    while let Some(item ) = iter4.next() {
      assert!(item.0.len() > 0);
      println!("{:?}", String::from_utf8(item.0.to_vec()));
    }

    // 有前缀的情况
    let mut iter_opts2 = IteratorOptions::default();
    iter_opts2.prefix = "c".as_bytes().to_vec();
    let mut iter5 = bt.iterator(iter_opts2);
    while let Some(item ) = iter5.next() {
      assert!(item.0.len() > 0);
      // println!("{:?}", String::from_utf8(item.0.to_vec()));
    }

    let mut iter_opts3 = IteratorOptions::default();
    iter_opts3.prefix = "csss".as_bytes().to_vec();
    let mut iter6 = bt.iterator(iter_opts3);
    let res6 = iter6.next();
    assert!(res6.is_none());

    let mut iter_opts4 = IteratorOptions::default();
    iter_opts4.prefix = "cadd".as_bytes().to_vec();
    let mut iter6 = bt.iterator(iter_opts4);
    while let Some(item ) = iter6.next() {
      assert!(item.0.len() > 0);
      // println!("{:?}", String::from_utf8(item.0.to_vec()));
    }
  }
}