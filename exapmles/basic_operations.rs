use bitcask_rs::db;
use bitcask_rs::options::Options;
use bitcask_rs::errors::Errors;
use bytes::Bytes;

fn main() {
    let opts = Options::default();
    let engine = db::Engine::open(opts).expect("failed to open bitcask engine");

    let res1 = engine.put(Bytes::from("name"), Bytes::from("bitcask-rs"));
    assert!(res1.is_ok());

    let res2 = engine.get(Bytes::from("name"));
    assert!(res2.is_ok());

    let val = res2.ok().unwrap();
    println!("val = {:?}", String::from_utf8(val.to_vec()));

    let res3 = engine.delete(Bytes::from("name"));
    assert!(res3.is_ok());

    let res4 = engine.get(Bytes::from("name"));
    assert!(res4.is_err());
    assert_eq!(res4.err().unwrap(), Errors::KeyNotFound);
}
