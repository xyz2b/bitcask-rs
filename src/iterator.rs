use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;

use crate::{db::Engine, errors::Result, index::IndexIterator, options::IteratorOptions};

/// 迭代器接口
pub struct Iterator<'a> {
    index_iter: Arc<RwLock<Box<dyn IndexIterator>>>, // 索引迭代器
    engine: &'a Engine,                              // engine的引用必须比Iterator寿命长
}

impl Engine {
    pub fn iter(&self, options: IteratorOptions) -> Iterator {
        Iterator {
            index_iter: Arc::new(RwLock::new(self.index.iterator(options))),
            engine: self,
        }
    }

    /// 返回数据库中所有的 kyes
    pub fn list_keys(&self) -> Result<Vec<Bytes>> {
        self.index.list_keys()
    }

    /// 对数据库中当中的所有数据执行函数操作，函数返回 false 时终止
    pub fn fold<F>(&self, f: F) -> Result<()>
    where
        Self: Sized,
        F: Fn(Bytes, Bytes) -> bool,
    {
        let mut iter = self.iter(IteratorOptions::default());
        while let Some((key, value)) = iter.next() {
            if !f(key, value) {
                break;
            }
        }

        Ok(())
    }
}

impl Iterator<'_> {
    /// 重新回到迭代器的起点，即第一个数据
    pub fn rewind(&mut self) {
        let mut index_iter = self.index_iter.write();
        index_iter.rewind();
    }

    /// 根据传入的 key 查找到第一个大于（或小于）等于的目标 key，从这个 key 开始遍历
    pub fn seek(&mut self, key: Vec<u8>) {
        let mut index_iter = self.index_iter.write();
        index_iter.seek(key);
    }

    /// 跳转到下一个 key，返回 None 说明遍历完成
    pub fn next(&mut self) -> Option<(Bytes, Bytes)> {
        let mut index_iter = self.index_iter.write();
        if let Some(item) = index_iter.next() {
            let value = self
                .engine
                .get_value_by_position(item.1)
                .expect("failed to get value from data file");
            return Some((Bytes::from(item.0.to_vec()), value));
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::{options::Options, util};
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn test_list_keys() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-list-keys");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let keys1 = engine.list_keys();
        assert!(keys1.is_ok());
        assert!(keys1.unwrap().is_empty());

        let put_res2 = engine.put(Bytes::from("eecc"), util::rand_kv::get_test_value(10));
        assert!(put_res2.is_ok());
        let put_res3 = engine.put(Bytes::from("bbac"), util::rand_kv::get_test_value(10));
        assert!(put_res3.is_ok());
        let put_res4 = engine.put(Bytes::from("ccde"), util::rand_kv::get_test_value(10));
        assert!(put_res4.is_ok());
        let keys2 = engine.list_keys();
        assert!(keys2.is_ok());
        assert_eq!(keys2.unwrap().len(), 3);

        // 删除测试的文件夹
        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_fold() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-fold");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let put_res2 = engine.put(Bytes::from("eecc"), util::rand_kv::get_test_value(10));
        assert!(put_res2.is_ok());
        let put_res3 = engine.put(Bytes::from("bbac"), util::rand_kv::get_test_value(10));
        assert!(put_res3.is_ok());
        let put_res4 = engine.put(Bytes::from("ccde"), util::rand_kv::get_test_value(10));
        assert!(put_res4.is_ok());

        engine
            .fold(|key, value| {
                println!("{:?}", key);
                println!("{:?}", value);
                return true;
            })
            .unwrap();

        engine
            .fold(|key, value| {
                println!("{:?}", key);
                println!("{:?}", value);

                if key.ge(&"bb") {
                    return false;
                }

                return true;
            })
            .unwrap();

        // 删除测试的文件夹
        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_iterator_seek() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-seek");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        // 没有数据的情况
        let mut iter1 = engine.iter(IteratorOptions::default());
        iter1.seek("aa".as_bytes().to_vec());
        let res1 = iter1.next();
        assert!(res1.is_none());
        // println!("{:?}", iter1.next());

        // 有一条数据的情况
        let put_res1 = engine.put(Bytes::from("aacc"), util::rand_kv::get_test_value(10));
        assert!(put_res1.is_ok());
        let mut iter2 = engine.iter(IteratorOptions::default());
        iter2.seek("a".as_bytes().to_vec());
        let res2 = iter2.next();
        assert!(res2.is_some());
        // println!("{:?}", res2);

        // 有多条数据
        let put_res2 = engine.put(Bytes::from("eecc"), util::rand_kv::get_test_value(10));
        assert!(put_res2.is_ok());
        let put_res3 = engine.put(Bytes::from("bbac"), util::rand_kv::get_test_value(10));
        assert!(put_res3.is_ok());
        let put_res4 = engine.put(Bytes::from("ccde"), util::rand_kv::get_test_value(10));
        assert!(put_res4.is_ok());

        let mut iter3 = engine.iter(IteratorOptions::default());
        iter3.seek("a".as_bytes().to_vec());
        let res3 = iter3.next();
        assert!(res3.is_some());
        assert_eq!(Bytes::from("aacc"), res3.unwrap().0);
        // println!("{:?}", res2);

        // 删除测试的文件夹
        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_iterator_next() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-next");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        // 有一条数据的情况
        let put_res1 = engine.put(Bytes::from("aacc"), util::rand_kv::get_test_value(10));
        assert!(put_res1.is_ok());
        let mut iter1 = engine.iter(IteratorOptions::default());
        let res1 = iter1.next();
        assert!(res1.is_some());
        assert_eq!(Bytes::from("aacc"), res1.unwrap().0);
        // println!("{:?}", res1);
        let res2 = iter1.next();
        assert!(res2.is_none());
        iter1.rewind();
        let res3 = iter1.next();
        assert!(res3.is_some());
        assert_eq!(Bytes::from("aacc"), res3.unwrap().0);
        let res4 = iter1.next();
        assert!(res4.is_none());

        // 有多条数据
        let put_res2 = engine.put(Bytes::from("eecc"), util::rand_kv::get_test_value(10));
        assert!(put_res2.is_ok());
        let put_res3 = engine.put(Bytes::from("bbac"), util::rand_kv::get_test_value(10));
        assert!(put_res3.is_ok());
        let put_res4 = engine.put(Bytes::from("ccde"), util::rand_kv::get_test_value(10));
        assert!(put_res4.is_ok());

        let mut iter2 = engine.iter(IteratorOptions::default());
        let res5 = iter2.next();
        // println!("{:?}", res5);
        assert!(res5.is_some());
        assert_eq!(Bytes::from("aacc"), res5.unwrap().0);
        let res6 = iter2.next();
        // println!("{:?}", res6);
        assert!(res6.is_some());
        assert_eq!(Bytes::from("bbac"), res6.unwrap().0);
        let res7 = iter2.next();
        assert!(res7.is_some());
        assert_eq!(Bytes::from("ccde"), res7.unwrap().0);
        let res8 = iter2.next();
        assert!(res8.is_some());
        assert_eq!(Bytes::from("eecc"), res8.unwrap().0);
        let res9 = iter2.next();
        assert!(res9.is_none());

        // 反向迭代
        let mut iter_opts1 = IteratorOptions::default();
        iter_opts1.reverse = true;
        let mut iter3 = engine.iter(iter_opts1);
        let res10 = iter3.next();
        // println!("{:?}", res10);
        assert!(res10.is_some());
        assert_eq!(Bytes::from("eecc"), res10.unwrap().0);
        let res11 = iter3.next();
        assert!(res11.is_some());
        assert_eq!(Bytes::from("ccde"), res11.unwrap().0);

        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_iterator_prefix() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-prefix");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let put_res1 = engine.put(Bytes::from("aacc"), util::rand_kv::get_test_value(10));
        assert!(put_res1.is_ok());
        let put_res2 = engine.put(Bytes::from("eecc"), util::rand_kv::get_test_value(10));
        assert!(put_res2.is_ok());
        let put_res3 = engine.put(Bytes::from("bbac"), util::rand_kv::get_test_value(10));
        assert!(put_res3.is_ok());
        let put_res4 = engine.put(Bytes::from("ccde"), util::rand_kv::get_test_value(10));
        assert!(put_res4.is_ok());

        let mut iter_opt1 = IteratorOptions::default();
        iter_opt1.prefix = "bb".as_bytes().to_vec();
        let mut iter1 = engine.iter(iter_opt1);

        let res1 = iter1.next();
        assert!(res1.is_some());
        assert_eq!(Bytes::from("bbac"), res1.unwrap().0);
        let res2 = iter1.next();
        assert!(res2.is_none());

        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
    }
}
