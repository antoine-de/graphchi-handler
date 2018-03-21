extern crate env_logger;
extern crate failure;
extern crate json;
#[macro_use]
extern crate log;
extern crate regex;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
#[macro_use]
extern crate structopt;
#[macro_use(stmt)]
extern crate cassandra_cpp;
extern crate futures;
extern crate itertools;

use failure::Error;
use std::path::PathBuf;
use structopt::StructOpt;
use std::fs::File;
use std::io::BufReader;
use std::io::prelude::*;
use std::env;
use futures::future::Future;
use regex::Regex;
use itertools::Itertools;

#[derive(StructOpt, Debug)]
struct Args {
    /// wat file
    #[structopt(short = "i", long = "input", parse(from_os_str))]
    input: PathBuf,
}

fn format_link(json: &json::JsonValue) -> String {
    format!("{{url:'{url}',text:'{text}',path:'{path}'}}", 
        url=json["url"].as_str().unwrap_or(""),
        text=json["text"].as_str().unwrap_or(""),
        path=json["path"].as_str().unwrap_or(""),
    )
}

fn parse_file(wat_file: PathBuf) -> Result<(), Error> {
    info!("reading {:?}", &wat_file);

    let f = File::open(&wat_file)?;
    let reader = BufReader::new(f);

    let match_record_id: Regex = Regex::new("\"WARC-Record-ID\":\"<urn:uuid:([^>]+)>\"").unwrap();
    let match_record_url: Regex = Regex::new("\"WARC-Target-URI\":\"([^\"]+)\"").unwrap();
    // to speed up a bit we don't parse the whole json at first, we parse only the links
    let match_links: Regex = Regex::new("\"Links\":([^]]+}])").unwrap();

    let mut cluster = cassandra_cpp::Cluster::default();
    cluster.set_contact_points("127.0.0.1").unwrap();
    cluster.set_load_balance_round_robin();

    let session = cluster.connect().unwrap();

    for l in reader.lines() {
        let l = l?;

        if !l.starts_with("{") {
            //TODO do not load whole line, check first char
            continue;
        }

        let base_id = match_record_id.captures(&l).map(|c| c[1].to_string());
        let base_url = match_record_url.captures(&l).map(|c| c[1].to_string());
        let links = &match_links.captures(&l).map(|c| c[1].to_string());

        if let &Some(ref l) = links {
            match json::parse(&l) {
                Ok(parsed_json) => {
                    let cassandra_links = format!("[{}]", parsed_json.members().into_iter().map(format_link).join(","));

                    let query = format!("INSERT INTO graph.sites (uuid, url, links) VALUES ({}, '{}', {})", 
                        base_id.unwrap_or("".into()), 
                        base_url.unwrap_or("".into()), 
                        cassandra_links
                    );

                    info!("query {}", &query);

                    let statement = cassandra_cpp::Statement::new(&query, 0);
                    let mut future = session.execute(&statement);
                    future.wait().unwrap();
                },
                Err(e) =>  {
                    warn!("error while parsing the json, skipping line (error: {})", e);
                }
            }
        }
    }

    session.close().wait().unwrap();
    Ok(())
}

fn main() {
    let mut builder = env_logger::Builder::new();
    builder.filter(None, log::LevelFilter::Info);
    if let Ok(s) = env::var("RUST_LOG") {
        builder.parse(&s);
    }
    builder.init();

    let args = Args::from_args();

    parse_file(args.input).unwrap();
}
