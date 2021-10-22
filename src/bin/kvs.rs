extern crate clap;

use clap::{App, Arg, SubCommand};
use std::process::exit;

fn main() {
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
        ("set", Some(_args)) => {
            eprintln!("unimplemented");
            exit(1);
        }
        ("get", Some(_args)) => {
            eprintln!("unimplemented");
            exit(1);
        }
        ("rm", Some(_args)) => {
            eprintln!("unimplemented");
            exit(1);
        }
        _ => {
            panic!()
        }
    };
}
