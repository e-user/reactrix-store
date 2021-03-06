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

mod mongo;
mod postgres;

use failure::Fail;
use log::warn;
pub use mongo::*;
pub use postgres::*;

#[derive(Debug, Fail)]
pub enum DataStoreError {
    #[fail(display = "Database error: {}", 0)]
    Database(String),
    #[fail(display = "Record not found")]
    NoRecord,
    #[fail(display = "Identical hash for different data detected: {}", 0)]
    Collision(String),
}

pub type Result<T> = std::result::Result<T, DataStoreError>;

fn entry_exists(store: &impl DataStore, hash: &[u8], data: &[u8]) -> Result<bool> {
    match store.retrieve(&hash) {
        Ok(stored) => {
            if data == &stored[..] {
                warn!("Data blob {} is already stored", &hex::encode(hash));
                return Ok(true);
            } else {
                return Err(DataStoreError::Collision(hex::encode(hash)));
            }
        }

        Err(DataStoreError::NoRecord) => Ok(false),
        Err(e) => Err(e),
    }
}

pub trait DataStore: Send + Sync {
    fn store(&self, data: &[u8]) -> Result<Vec<u8>>;
    fn retrieve(&self, id: &[u8]) -> Result<Vec<u8>>;
}
