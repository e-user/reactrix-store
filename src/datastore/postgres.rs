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
use crate::PgPool;

use blake2::{Blake2s, Digest};
use diesel::prelude::*;
use diesel::result::Error as DieselError;
use r2d2::Error as R2d2Error;
use reactrix::{schema, Data};
use std::sync::Arc;

pub struct PostgresDataStore(Arc<PgPool>);

impl PostgresDataStore {
    pub fn new(pool: Arc<PgPool>) -> Self {
        Self(pool)
    }
}

impl DataStore for PostgresDataStore {
    fn store(&self, data: &[u8]) -> Result<Vec<u8>> {
        let hash = Blake2s::digest(data);

        if entry_exists(self, &hash, data)? {
            return Ok(hash.to_vec());
        }

        match diesel::insert_into(schema::datastore::table)
            .values(Data {
                hash: hash.to_vec(),
                data: data.to_vec(),
            })
            .get_result::<Data>(&self.0.get()?)
        {
            Ok(data) => Ok(data.hash),
            Err(e) => Err(e.into()),
        }
    }

    fn retrieve(&self, id: &[u8]) -> Result<Vec<u8>> {
        use schema::datastore::dsl;

        match dsl::datastore
            .select(dsl::data)
            .filter(dsl::hash.eq(id))
            .first::<Vec<u8>>(&self.0.get()?)
        {
            Ok(data) => Ok(data),
            Err(DieselError::NotFound) => Err(DataStoreError::NoRecord),
            Err(e) => Err(e.into()),
        }
    }
}

impl From<DieselError> for DataStoreError {
    fn from(error: DieselError) -> Self {
        match error {
            DieselError::NotFound => Self::NoRecord,
            _ => Self::Database(error.to_string()),
        }
    }
}

impl From<R2d2Error> for DataStoreError {
    fn from(error: R2d2Error) -> Self {
        Self::Database(error.to_string())
    }
}
