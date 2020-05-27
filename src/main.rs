// This file is part of reactrix-store.
//
// Copyright 2019-2020 Alexander Dorn
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod datastore;
mod eventstore;
mod mq;

use bytes::Bytes;
use datastore::{DataStore, DataStoreError, MongoDataStore, PostgresDataStore};
use diesel::r2d2::{ConnectionManager, Pool};
use diesel::PgConnection;
use dotenv::dotenv;
use eventstore::{EventStore, EventStoreError, MongoEventStore, PostgresEventStore};
use exitfailure::ExitFailure;
use failure::Fail;
use log::{error, warn};
use mongodb::{options::ClientOptions, Client};
use mq::{Message, PublishMessage, Tx};
use reactrix::{ApiResult, NewEvent};
use serde::Serialize;
use std::convert::Infallible;
use std::env;
use std::net::Ipv4Addr;
use std::result::Result;
use std::sync::{Arc, Mutex};
use structopt::StructOpt;
use url::Url;
use warp::http::StatusCode;
use warp::{Filter, Reply};

type PgPool = Pool<ConnectionManager<PgConnection>>;

#[derive(Debug, Fail)]
pub enum ReactrixError {
    #[fail(display = "Environment variable {} is missing", 0)]
    Var(String),
    #[fail(display = "Unknown database type {}", 0)]
    UnknownDatabase(String),
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "kebab-case")]
struct Config {
    address: String,
    http_port: u16,
    zmq_port: u16,
}

fn error_response(reason: String, status: StatusCode) -> warp::reply::Response {
    warp::reply::with_status(
        warp::reply::json(&ApiResult::<String>::Error { reason }),
        status,
    )
    .into_response()
}

async fn config_get(config: Config) -> Result<impl Reply, Infallible> {
    Ok(warp::reply::json(&config))
}

async fn sequence_get(store: Arc<dyn EventStore>) -> Result<impl Reply, Infallible> {
    match store.sequence() {
        Ok(id) => Ok(warp::reply::json(&ApiResult::Ok { data: id }).into_response()),
        Err(e) => Ok(error_response(
            e.to_string(),
            StatusCode::INTERNAL_SERVER_ERROR,
        )),
    }
}

async fn event_get(sequence: i64, store: Arc<dyn EventStore>) -> Result<impl Reply, Infallible> {
    match store.retrieve(sequence) {
        Ok(event) => Ok(warp::reply::json(&ApiResult::Ok { data: event }).into_response()),
        Err(EventStoreError::NoRecord) => Ok(error_response(
            "No such event".to_string(),
            StatusCode::NOT_FOUND,
        )),
        Err(EventStoreError::Database(e)) => Ok(error_response(
            e.to_string(),
            StatusCode::INTERNAL_SERVER_ERROR,
        )),
    }
}

async fn event_put(
    event: NewEvent,
    store: Arc<dyn EventStore>,
    tx: Arc<Mutex<Tx>>,
) -> Result<impl Reply, Infallible> {
    match store.store(event) {
        Ok(i) => match tx.lock() {
            Ok(tx) => match tx.send(PublishMessage::Sequence(i)) {
                Ok(()) => Ok(warp::reply::with_status(
                    warp::reply::json(&ApiResult::Ok { data: i }),
                    StatusCode::CREATED,
                )
                .into_response()),
                Err(e) => {
                    let message = format!("Created but couldn't notify: {:?}", e);
                    error!("{}", &message);
                    Ok(error_response(message, StatusCode::INTERNAL_SERVER_ERROR))
                }
            },
            Err(e) => Ok(error_response(
                e.to_string(),
                StatusCode::INTERNAL_SERVER_ERROR,
            )),
        },
        Err(e) => Ok(error_response(
            e.to_string(),
            StatusCode::INTERNAL_SERVER_ERROR,
        )),
    }
}

async fn data_get(id: String, store: Arc<dyn DataStore>) -> Result<impl warp::Reply, Infallible> {
    let hash = match hex::decode(id.as_bytes()) {
        Ok(hash) => hash,
        Err(e) => {
            let message = format!("Couldn't decode hash: {}", e);
            warn!("{}", &message);
            return Ok(error_response(message, StatusCode::BAD_REQUEST));
        }
    };

    match store.retrieve(&hash) {
        Ok(data) => Ok(data.into_response()),
        Err(DataStoreError::NoRecord) => Ok(StatusCode::NOT_FOUND.into_response()),
        Err(e) => {
            let message = format!("Couldn't retrieve data hash {}: {}", id, e);
            error!("{}", &message);
            Ok(error_response(message, StatusCode::INTERNAL_SERVER_ERROR))
        }
    }
}

async fn data_put(bytes: Bytes, store: Arc<dyn DataStore>) -> Result<impl warp::Reply, Infallible> {
    match store.store(bytes.into_iter().collect::<Vec<u8>>().as_ref()) {
        Ok(hash) => Ok(hex::encode(hash).into_response()),
        Err(e) => {
            let message = format!("Couldn't store data: {}", e);
            error!("{}", &message);
            Ok(error_response(message, StatusCode::INTERNAL_SERVER_ERROR))
        }
    }
}

async fn message_post(
    topic: String,
    bytes: Bytes,
    tx: Arc<Mutex<Tx>>,
) -> Result<impl warp::Reply, Infallible> {
    let message = Message {
        topic,
        data: bytes.into_iter().collect::<Vec<u8>>(),
    };
    match tx.lock() {
        Ok(tx) => match tx.send(PublishMessage::Forward(message)) {
            Ok(()) => Ok(StatusCode::CREATED.into_response()),
            Err(e) => {
                let message = format!("Couldn't forward message: {:?}", e);
                error!("{}", &message);
                Ok(error_response(message, StatusCode::INTERNAL_SERVER_ERROR))
            }
        },
        Err(e) => Ok(error_response(
            e.to_string(),
            StatusCode::INTERNAL_SERVER_ERROR,
        )),
    }
}

fn database_url() -> Result<String, ReactrixError> {
    env::var("DATABASE_URL").or_else(|_| Err(ReactrixError::Var("DATABASE_URL".to_string())))
}

#[derive(Debug, StructOpt)]
#[structopt(
    raw(setting = "structopt::clap::AppSettings::ColoredHelp"),
    rename_all = "kebab-case"
)]
struct Cli {
    /// Address to listen on
    #[structopt(short, long, default_value = "127.0.0.1")]
    address: Ipv4Addr,

    /// HTTP port to listen on
    #[structopt(short = "p", long = "port", default_value = "8000")]
    http_port: u16,

    /// ØMQ port to listen on
    #[structopt(long, default_value = "5660")]
    zmq_port: u16,
}

async fn init_stores(url: &str) -> Result<(Arc<dyn EventStore>, Arc<dyn DataStore>), ExitFailure> {
    match Url::parse(url)?.scheme() {
        "postgres" => {
            let pool = Arc::new(Pool::new(ConnectionManager::<PgConnection>::new(url))?);
            Ok((
                Arc::new(PostgresEventStore::new(pool.clone())),
                Arc::new(PostgresDataStore::new(pool)),
            ))
        }
        "mongodb" => {
            let mut options = ClientOptions::parse(url).await?;
            options.app_name = Some("reactrix-store".to_string());
            let client = Client::with_options(options)?;
            let db = client.database("reactrix");
            Ok((
                Arc::new(MongoEventStore::new(db.clone())),
                Arc::new(MongoDataStore::new(db)),
            ))
        }
        s => Err(ReactrixError::UnknownDatabase(s.to_string()).into()),
    }
}

#[tokio::main]
async fn main() -> Result<(), ExitFailure> {
    let cli = Cli::from_args();
    dotenv()?;
    env_logger::builder().format_timestamp(None).init();

    let url = database_url()?;
    let (event_store, data_store) = init_stores(&url).await?;

    let event_store = warp::any().map(move || event_store.clone());
    let data_store = warp::any().map(move || data_store.clone());

    let tx = Arc::new(Mutex::new(mq::launch(cli.address, cli.zmq_port)?));
    let tx = warp::any().map(move || tx.clone());

    let prefix = warp::path!("v1" / ..);

    let config = Config {
        address: cli.address.to_string(),
        http_port: cli.http_port,
        zmq_port: cli.zmq_port,
    };

    let config_get = warp::path!("config")
        .and(warp::get())
        .map(move || config.clone())
        .and_then(config_get);

    let sequence_get = warp::path!("sequence")
        .and(warp::get())
        .and(event_store.clone())
        .and_then(sequence_get);

    let event_get = warp::path!("event" / i64)
        .and(warp::get())
        .and(event_store.clone())
        .and_then(event_get);

    let event_put = warp::path!("event")
        .and(warp::put())
        .and(warp::body::json())
        .and(event_store)
        .and(tx.clone())
        .and_then(event_put);

    let data_get = warp::path!("data" / String)
        .and(warp::get())
        .and(data_store.clone())
        .and_then(data_get);

    let data_put = warp::path!("data")
        .and(warp::put())
        .and(warp::body::bytes())
        .and(data_store)
        .and_then(data_put);

    let message_post = warp::path!("message" / String)
        .and(warp::post())
        .and(warp::body::bytes())
        .and(tx)
        .and_then(message_post);

    let api = prefix
        .and(
            config_get
                .or(sequence_get)
                .or(event_get)
                .or(event_put)
                .or(data_get)
                .or(data_put)
                .or(message_post),
        )
        .with(warp::log("reactrix"));

    warp::serve(api).run((cli.address, cli.http_port)).await;
    Ok(())
}
