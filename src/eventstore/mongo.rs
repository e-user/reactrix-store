// This file is part of reactrix-store.
//
// Copyright 2020 Alexander Dorn
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

use crate::eventstore::{EventStore, EventStoreError, Result};

use bson::ordered::ValueAccessError;
use bson::{doc, Bson, DecoderError};
use chrono::Utc;
use futures::executor::block_on;
use mongodb::error::Error as MongoError;
use mongodb::Database;
use reactrix::{Event, NewEvent};

pub struct MongoEventStore(Database);

impl MongoEventStore {
    pub fn new(database: Database) -> Self {
        Self(database)
    }
}

impl EventStore for MongoEventStore {
    fn store(&self, event: NewEvent) -> Result<i64> {
        let sequence = block_on(self.0.collection("counters").find_one_and_update(
            doc! { "_id": "events" },
            doc! { "$inc": { "sequence": 1 } },
            None,
        ))?
        .unwrap()
        .get_i64("sequence")?;

        match bson::to_bson(&event) {
            Ok(Bson::Document(mut doc)) => {
                doc.insert("sequence", sequence);
                doc.insert("timestamp", Utc::now());
                block_on(self.0.collection("events").insert_one(doc, None))?;
                Ok(sequence)
            }

            Ok(_) => Err(EventStoreError::Database(
                "Could not properly convert JSON to BSON".to_string(),
            )),
            Err(e) => Err(EventStoreError::Database(e.to_string())),
        }
    }

    fn retrieve(&self, id: i64) -> Result<Event> {
        match block_on(
            self.0
                .collection("events")
                .find_one(doc! { "sequence": id }, None),
        ) {
            Ok(Some(ref doc)) if doc.contains_key(&"$err") => {
                Err(EventStoreError::Database(doc.get_str(&"$err")?.to_owned()))
            }
            Ok(Some(doc)) => Ok(Event {
                sequence: doc.get_i64("sequence")?,
                version: doc.get_i32("version")?,
                data: Bson::Document(doc.get_document("data")?.clone()).into(),
                timestamp: *doc.get_utc_datetime("timestamp")?,
            }),
            Ok(None) => Err(EventStoreError::NoRecord),
            Err(e) => Err(EventStoreError::Database(e.to_string())),
        }
    }
}

impl From<MongoError> for EventStoreError {
    fn from(error: MongoError) -> Self {
        Self::Database(error.to_string())
    }
}

impl From<ValueAccessError> for EventStoreError {
    fn from(error: ValueAccessError) -> Self {
        Self::Database(error.to_string())
    }
}

impl From<DecoderError> for EventStoreError {
    fn from(error: DecoderError) -> Self {
        Self::Database(format!(
            "Couldn't convert BSON value: {}",
            &error.to_string()
        ))
    }
}
