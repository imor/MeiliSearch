use std::collections::{BTreeSet, HashSet};

use http::StatusCode;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tide::querystring::ContextExt as QSContextExt;
use tide::response::IntoResponse;
use tide::{Context, Response};

use crate::error::{ResponseError, SResult};
use crate::helpers::tide::ContextExt;
use crate::helpers::tide::ACL::*;
use crate::Data;

pub async fn get_document(ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(Public)?;

    let index = ctx.index()?;

    let identifier = ctx.identifier()?;
    let document_id = meilisearch_core::serde::compute_document_id(identifier.clone());

    let db = &ctx.state().db;
    let reader = db.main_read_txn().map_err(ResponseError::internal)?;

    let response = index
        .document::<IndexMap<String, Value>>(&reader, None, document_id)
        .map_err(ResponseError::internal)?
        .ok_or(ResponseError::document_not_found(&identifier))?;

    if response.is_empty() {
        return Err(ResponseError::document_not_found(identifier));
    }

    Ok(tide::response::json(response))
}

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexUpdateResponse {
    pub update_id: u64,
}

pub async fn delete_document(ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(Private)?;

    let index = ctx.index()?;
    let identifier = ctx.identifier()?;
    let document_id = meilisearch_core::serde::compute_document_id(identifier.clone());

    let db = &ctx.state().db;
    let mut update_writer = db.update_write_txn().map_err(ResponseError::internal)?;

    let mut documents_deletion = index.documents_deletion();
    documents_deletion.delete_document_by_id(document_id);
    let update_id = documents_deletion
        .finalize(&mut update_writer)
        .map_err(ResponseError::internal)?;

    update_writer.commit().map_err(ResponseError::internal)?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::response::json(response_body)
        .with_status(StatusCode::ACCEPTED)
        .into_response())
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BrowseQuery {
    offset: Option<usize>,
    limit: Option<usize>,
    attributes_to_retrieve: Option<String>,
}

pub async fn get_all_documents(ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(Public)?;

    let index = ctx.index()?;
    let query: BrowseQuery = ctx.url_query().unwrap_or(BrowseQuery::default());

    let offset = query.offset.unwrap_or(0);
    let limit = query.limit.unwrap_or(20);

    let db = &ctx.state().db;
    let reader = db.main_read_txn().map_err(ResponseError::internal)?;

    let documents_ids: Result<BTreeSet<_>, _> =
        match index.documents_fields_counts.documents_ids(&reader) {
            Ok(documents_ids) => documents_ids.skip(offset).take(limit).collect(),
            Err(e) => return Err(ResponseError::internal(e)),
        };

    let documents_ids = match documents_ids {
        Ok(documents_ids) => documents_ids,
        Err(e) => return Err(ResponseError::internal(e)),
    };

    let mut response_body = Vec::<IndexMap<String, Value>>::new();

    if let Some(attributes) = query.attributes_to_retrieve {
        let attributes = attributes.split(',').collect::<HashSet<&str>>();
        for document_id in documents_ids {
            if let Ok(Some(document)) = index.document(&reader, Some(&attributes), document_id) {
                response_body.push(document);
            }
        }
    } else {
        for document_id in documents_ids {
            if let Ok(Some(document)) = index.document(&reader, None, document_id) {
                response_body.push(document);
            }
        }
    }

    Ok(tide::response::json(response_body))
}

fn infered_schema(document: &IndexMap<String, Value>) -> Option<meilisearch_schema::Schema> {
    use meilisearch_schema::{SchemaBuilder, DISPLAYED, INDEXED};

    let mut identifier = None;
    for key in document.keys() {
        if identifier.is_none() && key.to_lowercase().contains("id") {
            identifier = Some(key);
        }
    }

    match identifier {
        Some(identifier) => {
            let mut builder = SchemaBuilder::with_identifier(identifier);
            for key in document.keys() {
                builder.new_attribute(key, DISPLAYED | INDEXED);
            }
            Some(builder.build())
        }
        None => None,
    }
}

async fn update_multiple_documents(mut ctx: Context<Data>, is_partial: bool) -> SResult<Response> {
    ctx.is_allowed(Private)?;

    let data: Vec<IndexMap<String, Value>> =
        ctx.body_json().await.map_err(ResponseError::bad_request)?;
    let index = ctx.index()?;

    let db = &ctx.state().db;
    let reader = db.main_read_txn().map_err(ResponseError::internal)?;
    let mut update_writer = db.update_write_txn().map_err(ResponseError::internal)?;

    let current_schema = index
        .main
        .schema(&reader)
        .map_err(ResponseError::internal)?;
    if current_schema.is_none() {
        match data.first().and_then(infered_schema) {
            Some(schema) => {
                index
                    .schema_update(&mut update_writer, schema)
                    .map_err(ResponseError::internal)?;
            }
            None => return Err(ResponseError::bad_request("Could not infer a schema")),
        }
    }

    let mut document_addition = if is_partial {
        index.documents_partial_addition()
    } else {
        index.documents_addition()
    };

    for document in data {
        document_addition.update_document(document);
    }

    let update_id = document_addition
        .finalize(&mut update_writer)
        .map_err(ResponseError::internal)?;

    update_writer.commit().map_err(ResponseError::internal)?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::response::json(response_body)
        .with_status(StatusCode::ACCEPTED)
        .into_response())
}

pub async fn add_or_replace_multiple_documents(ctx: Context<Data>) -> SResult<Response> {
    update_multiple_documents(ctx, false).await
}

pub async fn add_or_update_multiple_documents(ctx: Context<Data>) -> SResult<Response> {
    update_multiple_documents(ctx, true).await
}

pub async fn delete_multiple_documents(mut ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(Private)?;

    let data: Vec<Value> = ctx.body_json().await.map_err(ResponseError::bad_request)?;
    let index = ctx.index()?;

    let db = &ctx.state().db;
    let mut writer = db.update_write_txn().map_err(ResponseError::internal)?;

    let mut documents_deletion = index.documents_deletion();

    for identifier in data {
        if let Some(identifier) = meilisearch_core::serde::value_to_string(&identifier) {
            documents_deletion
                .delete_document_by_id(meilisearch_core::serde::compute_document_id(identifier));
        }
    }

    let update_id = documents_deletion
        .finalize(&mut writer)
        .map_err(ResponseError::internal)?;

    writer.commit().map_err(ResponseError::internal)?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::response::json(response_body)
        .with_status(StatusCode::ACCEPTED)
        .into_response())
}

pub async fn clear_all_documents(ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(Private)?;

    let index = ctx.index()?;

    let db = &ctx.state().db;
    let mut writer = db.update_write_txn().map_err(ResponseError::internal)?;

    let update_id = index
        .clear_all(&mut writer)
        .map_err(ResponseError::internal)?;
    writer.commit().map_err(ResponseError::internal)?;

    let response_body = IndexUpdateResponse { update_id };
    Ok(tide::response::json(response_body)
        .with_status(StatusCode::ACCEPTED)
        .into_response())
}
