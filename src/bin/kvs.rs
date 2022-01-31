use std::env;
use std::process::exit;

use clap::{App, Arg, SubCommand};

use kvs::{KvStore, KvsError, Result};

fn main() -> Result<()> {
    let storage_dir = env::current_dir()?;
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .subcommand(
            SubCommand::with_name("set")
                .about("Set the value of a string key to a string")
                .args(&[
                    Arg::with_name("KEY").required(true),
                    Arg::with_name("VALUE").required(true),
                ]),
        )
        .subcommand(
            SubCommand::with_name("get")
                .about("Get the string value of a given string key")
                .arg(Arg::with_name("KEY").required(true)),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .about("Remove a given key")
                .arg(Arg::with_name("KEY").required(true)),
        )
        .get_matches();

    match matches.subcommand() {
        ("set", Some(args)) => {
            let mut kvs = KvStore::open(storage_dir)?;
            kvs.set(
                args.value_of("KEY").unwrap().to_string(),
                args.value_of("VALUE").unwrap().to_string(),
            )?;
            exit(0);
        }
        ("get", Some(args)) => {
            let mut kvs = KvStore::open(storage_dir)?;
            if let Some(val) = kvs.get(args.value_of("KEY").unwrap().to_string())? {
                println!("{}", val);
            } else {
                println!("Key not found");
            }
            exit(0);
        }
        ("rm", Some(args)) => {
            let mut kvs = KvStore::open(storage_dir)?;
            match kvs.remove(args.value_of("KEY").unwrap().to_string()) {
                Ok(()) => exit(0),
                Err(KvsError::KeyNotFound) => {
                    println!("Key not found");
                    exit(1);
                }
                Err(e) => return Err(e),
            };
        }
        _ => {
            eprintln!("Invalid argument");
            exit(1);
        }
    };
}
