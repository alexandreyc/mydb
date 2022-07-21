use mydb::MyDB;
use std::fs;
use std::path;

#[test]
fn test_basic() {
    let file = tempfile::Builder::new()
        .append(true)
        .tempfile()
        .unwrap()
        .into_file();
    let mut db = MyDB::new_from_file(file).unwrap();

    assert_eq!(db.get("unknown_key").unwrap(), None);

    db.set("hello", "world").unwrap();
    assert_eq!(db.get("hello").unwrap(), Some("world".to_string()));

    db.set("hello", "mars").unwrap();
    assert_eq!(db.get("hello").unwrap(), Some("mars".to_string()));

    db.set("foo", "bar").unwrap();
    assert_eq!(db.get("foo").unwrap(), Some("bar".to_string()));
}

#[test]
fn test_load() {
    let filename = "test.db";
    if path::Path::new(filename).exists() {
        panic!(
            "test database file {} already exists, please delete it",
            filename
        );
    }

    let mut db = MyDB::new(filename).unwrap();
    db.set("hello", "world").unwrap();
    db.set("foo", "bar").unwrap();
    db.set("bar", "foo").unwrap();
    db.set("hello", "mars").unwrap();
    drop(db);

    let mut db = MyDB::new(filename).unwrap();
    assert_eq!(db.get("foo").unwrap(), Some("bar".to_string()));
    assert_eq!(db.get("bar").unwrap(), Some("foo".to_string()));
    assert_eq!(db.get("hello").unwrap(), Some("mars".to_string()));

    fs::remove_file(filename).unwrap();
}
