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

use crate::datastore::{entry_exists, DataStore, DataStoreError, Result};

use blake2::{Blake2s, Digest};
use bson::doc;
use bson::ordered::ValueAccessError;
use bson::spec::BinarySubtype;
use futures::executor::block_on;
use mongodb::error::Error as MongoError;
use mongodb::Database;

pub struct MongoDataStore(Database);

impl MongoDataStore {
    pub fn new(database: Database) -> Self {
        Self(database)
    }
}

impl DataStore for MongoDataStore {
    fn store(&self, data: &[u8]) -> Result<Vec<u8>> {
        let hash = Blake2s::digest(data);

        if entry_exists(self, &hash, data)? {
            return Ok(hash.to_vec());
        }

        let doc =
            doc! { "_id": hex::encode(hash), "data": (BinarySubtype::Generic, data.to_owned()) };

        block_on(self.0.collection("data").insert_one(doc, None))?;

        Ok(hash.to_vec())
    }

    fn retrieve(&self, id: &[u8]) -> Result<Vec<u8>> {
        let hash = hex::encode(id);

        match block_on(
            self.0
                .collection("data")
                .find_one(doc! { "_id": hash }, None),
        ) {
            Ok(Some(ref doc)) if doc.contains_key(&"$err") => {
                Err(DataStoreError::Database(doc.get_str(&"$err")?.to_owned()))
            }
            Ok(Some(doc)) => Ok(doc.get_binary_generic("data")?.to_owned()),
            Ok(None) => Err(DataStoreError::NoRecord),
            Err(e) => Err(DataStoreError::Database(e.to_string())),
        }
    }
}

impl From<MongoError> for DataStoreError {
    fn from(error: MongoError) -> Self {
        Self::Database(error.to_string())
    }
}

impl From<ValueAccessError> for DataStoreError {
    fn from(error: ValueAccessError) -> Self {
        Self::Database(error.to_string())
    }
}
