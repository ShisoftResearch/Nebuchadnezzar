use neb::server::*;
use neb::ram::schema::Schemas;
use std::rc::Rc;
use concurrent_hashmap::ConcHashMap;

#[test]
pub fn init () {
    let server = Server::new(ServerOptions {
        chunk_count: 1,
        memory_size: 1000,
        schemas: Schemas::new()
    });
}

#[test]
pub fn rc_hashmap (){
    let map = ConcHashMap::<u64, u64>::new();
    map.insert(1, 2);
    if let Some(val) = map.find(&1) {
        assert!(val.get() == &2);
    }
    let rc1 = Rc::new(map);
    let rc2 = rc1.clone();
    let rc3 = rc2.clone();
    {
        let rc4 = rc1.clone();
        rc4.insert(2, 3);
    }
    if let Some(val) = rc1.find(&2) {
        assert!(val.get() == &3);
    }
    rc2.insert(3, 4);
    if let Some(val) = rc3.find(&3) {
        assert!(val.get() == &4);
    }
    if let Some(val) = rc3.find(&2) {
        assert!(val.get() == &3);
    }
    println!("workaround");
}