// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use diesel::{
    ExpressionMethods,
    Insertable,
    OptionalExtension,
    QueryDsl,
    Queryable,
    QueryableByName,
    Selectable,
};
use diesel_async::{AsyncPgConnection, RunQueryDsl};

use crate::event::events::{CheckpointEventPosition, EventStreamCursor, EventStreamElement};

#[derive(Debug, Queryable, Selectable, Insertable)]
#[diesel(table_name = crate::backup::schema::stream_event)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct StreamEvent {
    pub checkpoint_sequence_number: i64,
    pub counter: i64,
    pub transaction_digest: Vec<u8>,
    pub event_index: i64,
    pub element_index: i64,
    pub element: serde_json::Value,
}

impl StreamEvent {
    pub fn new(
        checkpoint_event_position: CheckpointEventPosition,
        transaction_digest: [u8; 32],
        event_index: u64,
        element_index: u64,
        element: &EventStreamElement,
    ) -> Self {
        Self {
            element_index: element_index.try_into().expect("element_index overflow"),
            checkpoint_sequence_number: checkpoint_event_position
                .checkpoint_sequence_number
                .try_into()
                .expect("checkpoint_sequence_number overflow"),
            counter: checkpoint_event_position
                .counter
                .try_into()
                .expect("counter overflow"),
            transaction_digest: transaction_digest.into(),
            event_index: event_index.try_into().expect("event_index overflow"),
            element: serde_json::to_value(element).expect("failed to deserialize JSON"),
        }
    }
}

pub async fn get_backup_node_cursor(
    pg_connection: &mut AsyncPgConnection,
) -> anyhow::Result<EventStreamCursor> {
    use super::{models::StreamEvent, schema::stream_event::dsl};
    if let Some(stream_event) = dsl::stream_event
        .order(dsl::element_index.desc())
        .first::<StreamEvent>(pg_connection)
        .await
        .optional()?
    {
        return Ok(EventStreamCursor::new(
            None,
            (stream_event.element_index + 1)
                .try_into()
                .expect("underflow in element_index"),
        ));
    }

    tracing::warn!(
        "no db connection or no prior stream cursor. attempting to start from the beginning"
    );
    Ok(EventStreamCursor::new(None, 0))
}

#[derive(Debug, Queryable, Selectable, QueryableByName)]
#[diesel(table_name = crate::backup::schema::blob_state)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct BlobIdRow {
    pub blob_id: Vec<u8>,
}
