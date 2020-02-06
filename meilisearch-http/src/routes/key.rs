use tide::{Context, Response};

use crate::error::SResult;
use crate::helpers::tide::ContextExt;
use crate::helpers::tide::ACL::*;
use crate::Data;

pub async fn list(ctx: Context<Data>) -> SResult<Response> {
    ctx.is_allowed(Admin)?;

    let keys = &ctx.state().api_keys;

    Ok(tide::response::json(serde_json::json!({
        "private": keys.private,
        "public": keys.public,
    })))
}
