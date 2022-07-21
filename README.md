# MyDB

A simple key-value database implementation from scratch in Rust under 300 lines of code.

More specifically, it is a reimplemation in Rust of the didactic database [CaskDB](https://github.com/avinassh/py-caskdb) which is itself inspired by [Bitcask](https://riak.com/assets/bitcask-intro.pdf).

## Features

- Key/Value data model
- Embedded (use as a library, no client/server)
- Disk-based persistence
- No runtime dependencies

## Limitations

- Keys and values are limited to UTF-8 encoded strings
- No delete operation
- No range queries
- Memory usage might be high with a lot of keys
- Startup time might be slow because we need to load all keys from disk to memory

## Usage

```rust
use mydb::{MyDB, Result};

fn main() -> Result<()> {
    let filename = "data.db";
    let mut db = MyDB::new(filename)?;

    db.set("hello", "world")?;
    assert_eq!(db.get("hello")?, Some("world".to_string());

    assert_eq!(db.get("nokey")?, None);

    Ok(())
}
```

## Development

Run tests:

```bash
cargo test
```

Build for release:

```bash
cargo build --release
```

## License

See [LICENSE](./LICENSE).
