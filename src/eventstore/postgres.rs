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
use crate::PgPool;

use diesel::prelude::*;
use diesel::result::Error as DieselError;
use r2d2::Error as R2d2Error;
use reactrix::{schema, Event, NewEvent};
use std::sync::Arc;

pub struct PostgresEventStore(Arc<PgPool>);

impl PostgresEventStore {
    pub fn new(pool: Arc<PgPool>) -> Self {
        Self(pool)
    }
}

impl EventStore for PostgresEventStore {
    fn store(&self, event: NewEvent) -> Result<i64> {
        let result = diesel::insert_into(schema::events::table)
            .values::<NewEvent>(event)
            .get_result::<reactrix::Event>(&self.0.get()?)?;
        Ok(result.sequence)
    }

    fn retrieve(&self, id: i64) -> Result<Event> {
        use schema::events::dsl;

        Ok(dsl::events
            .filter(dsl::sequence.eq(id))
            .first::<Event>(&self.0.get()?)?)
    }
}

impl From<DieselError> for EventStoreError {
    fn from(error: DieselError) -> Self {
        match error {
            DieselError::NotFound => Self::NoRecord,
            _ => Self::Database(error.to_string()),
        }
    }
}

impl From<R2d2Error> for EventStoreError {
    fn from(error: R2d2Error) -> Self {
        Self::Database(error.to_string())
    }
}
