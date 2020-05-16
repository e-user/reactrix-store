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
pub use mongo::*;
pub use postgres::*;
use reactrix::{Event, NewEvent};

#[derive(Debug, Fail)]
pub enum EventStoreError {
    #[fail(display = "Database error: {}", 0)]
    Database(String),
    #[fail(display = "Record not found")]
    NoRecord,
}

pub type Result<T> = std::result::Result<T, EventStoreError>;

pub trait EventStore: Send + Sync {
    fn store(&self, data: NewEvent) -> Result<i64>;
    fn retrieve(&self, id: i64) -> Result<Event>;
}
