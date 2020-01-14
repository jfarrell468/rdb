use std::io;
use std::path;
use std::collections::BTreeMap;
use std::fs::File;

extern crate commitlog;
extern crate serde;
extern crate serde_cbor;
extern crate sstable;

use commitlog::*;
use serde::{Serialize, Deserialize};
use commitlog::message::MessageSet;
use sstable::SSIterator;
use std::cmp::Ordering;

// TODO: Split into "command" and "query"
#[derive(Serialize, Deserialize, Debug)]
enum Command {
    SELECT(String),
    INSERT { key: String, val: String },
    DELETE(String),
    DUMP,
    COMPACT,
    EXIT,
}

impl Command {
    fn parse(cmd: &String) -> Option<Command> {
        let mut tokens = cmd.split_whitespace();
        match tokens.next() {
            Some(cmd) => {
                match cmd {
                    ".dump" => Some(Command::DUMP),
                    ".exit" => Some(Command::EXIT),
                    ".compact" => Some(Command::COMPACT),
                    _ => match tokens.next() {
                        Some(arg) => {
                            match cmd {
                                "select" | "SELECT" => Some(Command::SELECT(arg.to_string())),
                                "insert" | "INSERT" => match tokens.next() {
                                    None => None,
                                    Some(val) => Some(Command::INSERT { key: arg.to_string(), val: val.to_string() }),
                                }
                                "delete" | "DELETE" => Some(Command::DELETE(arg.to_string())),
                                _ => None,
                            }
                        }
                        None => None,
                    },
                }
            }
            None => None,
        }
    }
    fn execute(&self, db: &mut DatabaseTable) {
        match self {
            Command::SELECT(key) => match db.select(key) {
                None => { println!("Not found") }
                Some(val) => { println!("{}", val) }
            }
            Command::INSERT { key, val } => {
                if db.insert(key, val) {
                    println!("Succeeded")
                } else {
                    println!("Duplicate key")
                }
            }
            Command::DELETE(key) => {
                if db.delete(key) {
                    println!("Succeeded")
                } else {
                    println!("Not found")
                }
            }
            _ => ()
        }
    }
    fn key(&self) -> Option<&String> {
        match self {
            Command::SELECT(key) => Some(key),
            Command::INSERT { key, val: _ } => Some(key),
            Command::DELETE(key) => Some(key),
            _ => None
        }
    }
}

trait DatabaseTable {
    fn insert(&mut self, key: &str, val: &str) -> bool;
    fn select(&self, key: &str) -> Option<String>;
    fn delete(&mut self, key: &str) -> bool;
}

#[derive(Debug)]
struct InMemoryTable {
    db: BTreeMap<String, String>
}

impl InMemoryTable {
    fn new() -> InMemoryTable {
        InMemoryTable { db: BTreeMap::new() }
    }
    fn dump(&self) {
        println!("{:?}", self)
    }
}

impl DatabaseTable for InMemoryTable {
    fn select(&self, key: &str) -> Option<String> {
        match self.db.get(key) {
            None => None,
            Some(x) => Some(x.to_string()),
        }
    }
    fn insert(&mut self, key: &str, val: &str) -> bool {
        if self.db.contains_key(key) {
            return false;
        } else {
            self.db.insert(key.to_string(), val.to_string());
            return true;
        }
    }
    fn delete(&mut self, key: &str) -> bool {
        if self.db.contains_key(key) {
            self.db.remove(key);
            return true;
        } else {
            return false;
        }
    }
}

const COMMITLOG: &str = "commitlog";
const SSTABLE: &str = "sstable";
const SSTABLE_NEW: &str = "sstable.new";
const SSTABLE_OLD: &str = "sstable.old";

struct DiskTable {
    path: path::PathBuf,
    commitlog: commitlog::CommitLog,
    mutations: BTreeMap<String, Vec<Command>>,
    sstable: sstable::Table,
}

impl DiskTable {
    fn new(path: &path::Path) -> DiskTable {
        if (!path.exists()) {
            std::fs::create_dir(path).expect("Failed to create directory for database");
        }
        let sstable_path = path.join(SSTABLE);
        if !sstable_path.exists() {
            let builder = sstable::TableBuilder::new(sstable::Options::default(), File::create(sstable_path.as_path()).expect("Failed to create empty sstable"));
            builder.finish().expect("Failed to finalize empty sstable");
        }
        let mut disktable = DiskTable {
            path: path.to_path_buf(),
            commitlog: CommitLog::new(LogOptions::new(path.join(COMMITLOG))).expect("Failed to open commitlog"),
            mutations: Default::default(),
            sstable: sstable::Table::new_from_file(sstable::Options::default(), sstable_path.as_path()).expect("Failed to open sstable"),
        };
        let messages = disktable.commitlog.read(0, ReadLimit::default()).expect("Failed to read commitlog.");
        for msg in messages.iter() {
            let cmd: Command = serde_cbor::from_slice(msg.payload()).expect("Failed to deserialize");
            match cmd.key() {
                // Fail silently.
                None => {},
                Some(key) => {
                    // Why do we have to copy key here? See https://internals.rust-lang.org/t/pre-rfc-abandonning-morals-in-the-name-of-performance-the-raw-entry-api/7043
                    disktable.mutations.entry(key.to_string()).or_insert(Default::default()).push(cmd);
                },
            }
        }
        return disktable
    }
    fn dump(&self) {
        println!("{:?}", self.mutations)
    }
    fn compact_key(&self, key: &str) -> Option<String> {
        let mut final_val = match self.sstable.get(key.as_bytes()) {
            Ok(val) => match val {
                None => None,
                Some(x) => Some(String::from_utf8(x).expect("Failed to decode UTF-8")),
            },
            Err(_) => None,
        };
        match self.mutations.get(key) {
            None => final_val,
            Some(cmds) => {
                for cmd in cmds {
                    match cmd {
                        Command::INSERT { key: _, val } => {final_val = Some(val.to_string())},
                        Command::DELETE(_) => {final_val = None},
                        _ => ()
                    }
                }
                final_val
            },
        }
    }
    fn compact(&mut self) {
        // Algorithm:
        // * Create a new sstable.
        // * Iterate over old sstable and mutations, and write new sstable.
        // * Swap the sstables and delete the old one.
        // * Truncate the commitlog.
        let mut builder = sstable::TableBuilder::new(sstable::Options::default(), File::create(self.path.join(SSTABLE_NEW)).expect("Failed to create sstable"));
        // sstable doesn't implement a standard iteration. grr.
        let mut sstable_iter = self.sstable.iter();
        let mut mut_iter = self.mutations.iter();
        loop {
            match sstable_iter.next() {
                // Precondition:
                None => {
                    loop {
                        match mut_iter.next() {
                            None => break,
                            Some(x) => {
                                let mut_key = x.0;
                                match self.compact_key(mut_key) {
                                    // We have a commitlog, but the key was ultimately deleted.
                                    None => (),
                                    Some(val) => {
                                        builder.add(mut_key.as_bytes(), val.as_bytes()).expect(format!("Failed to append to sstable. key = {}", mut_key).as_str());
                                    },
                                }
                            },
                        }
                    }
                    break
                },
                Some(x) => {
                    let sstable_key = String::from_utf8(x.0).expect("Failed to decode UTF-8");
                    loop {
                        match mut_iter.next() {
                            None => break,
                            Some(x) => {
                                match x.0.cmp(&sstable_key) {
                                    Ordering::Less => {
                                        let mut_key = x.0;
                                        match self.compact_key(mut_key) {
                                            // We have a commitlog, but the key was ultimately deleted.
                                            None => (),
                                            Some(val) => {
                                                builder.add(mut_key.as_bytes(), val.as_bytes()).expect(format!("Failed to append to sstable. key = {}", mut_key).as_str());
                                            },
                                        }
                                    },
                                    _ => break
                                }
                            }
                        }
                    }
                    builder.add(sstable_key.as_bytes(), self.compact_key(sstable_key.as_str()).expect("Failed to get key").as_bytes()).expect("Failed to append to sstable");
                },
            }
        }
        builder.finish().expect("Failed to call finish on sstable.");
        std::fs::rename(self.path.join(SSTABLE), self.path.join(SSTABLE_OLD)).expect("Failed to rename cur to old");
        std::fs::rename(self.path.join(SSTABLE_NEW), self.path.join(SSTABLE)).expect("Failed to rename new to cur");
        std::fs::remove_file(self.path.join(SSTABLE_OLD)).expect("Failed to remove old sstable");
        self.sstable = sstable::Table::new_from_file(sstable::Options::default(), self.path.join(SSTABLE).as_path()).expect("Failed to open sstable");
        // Note: self.commitlog.truncate(0) doesn't work. It leaves one element in the commitlog.
        std::fs::remove_dir_all(self.path.join(COMMITLOG)).expect("Failed to remove commitlog");
        self.commitlog = CommitLog::new(LogOptions::new(self.path.join(COMMITLOG))).expect("Failed to open commitlog");
        self.mutations.clear();
    }
}

impl DatabaseTable for DiskTable {
    fn insert(&mut self, key: &str, val: &str) -> bool {
        match self.compact_key(key) {
            Some(_) => false,
            None => {
                let cmd = Command::INSERT {key:key.to_string(), val: val.to_string()};
                self.commitlog.append_msg(serde_cbor::to_vec(&cmd).expect("Failed to serialize")).expect("Failed to append to commitlog");
                self.mutations.entry(key.to_string()).or_insert(Default::default()).push(cmd);
                self.commitlog.flush().expect("failed to flush commitlog");
                true
            }
        }
    }

    fn select(&self, key: &str) -> Option<String> {
        self.compact_key(key)
    }

    fn delete(&mut self, key: &str) -> bool {
        match self.compact_key(key) {
            Some(_) => {
                let cmd = Command::DELETE(key.to_string());
                self.commitlog.append_msg(serde_cbor::to_vec(&cmd).expect("Failed to serialize")).expect("Failed to append to commitlog");
                self.mutations.entry(key.to_string()).or_insert(Default::default()).push(cmd);
                self.commitlog.flush().expect("failed to flush commitlog");
                true
            },
            None => false
        }
    }
}

fn main() {
    println!("Enter a command");
    let mut db = DiskTable::new(path::Path::new("foobar"));
    loop {
        let mut line = String::new();
        io::stdin().read_line(&mut line).expect("Failed to read line");
//        println!("Your command was: {}", line);
        match Command::parse(&line) {
            None => { println!("Unrecognized command. Use .exit to quit.") }
            Some(cmd) => {
                println!("{:?}", cmd);
                match cmd {
                    Command::COMPACT => db.compact(),
                    Command::DUMP => db.dump(),
                    Command::EXIT => break,
                    _ => cmd.execute(&mut db)
                }
            }
        }
    }
}
