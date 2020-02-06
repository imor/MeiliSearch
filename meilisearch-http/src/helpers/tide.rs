use crate::error::{ResponseError, SResult};
use crate::Data;
use meilisearch_core::Index;
use tide::Context;

pub trait ContextExt {
    fn is_allowed(&self, acl: ACL) -> SResult<()>;
    fn header(&self, name: &str) -> Result<String, ResponseError>;
    fn url_param(&self, name: &str) -> Result<String, ResponseError>;
    fn index(&self) -> Result<Index, ResponseError>;
    fn identifier(&self) -> Result<String, ResponseError>;
}

pub enum ACL {
    Admin,
    Private,
    Public
}

impl ContextExt for Context<Data> {
    fn is_allowed(&self, acl: ACL) -> SResult<()> {
        let user_api_key = self.header("X-Meili-API-Key")?;

        match acl {
            ACL::Admin => {
                if Some(user_api_key.clone()) == self.state().api_keys.master {
                    return Ok(())
                }
            },
            ACL::Private => {
                if Some(user_api_key.clone()) == self.state().api_keys.master {
                    return Ok(())
                }
                if Some(user_api_key.clone()) == self.state().api_keys.private {
                    return Ok(())
                }
            },
            ACL::Public => {
                if Some(user_api_key.clone()) == self.state().api_keys.master {
                    return Ok(())
                }
                if Some(user_api_key.clone()) == self.state().api_keys.private {
                    return Ok(())
                }
                if Some(user_api_key.clone()) == self.state().api_keys.public {
                    return Ok(())
                }
            }
        }

        Err(ResponseError::InvalidToken(user_api_key.to_string()))
    }

    fn header(&self, name: &str) -> Result<String, ResponseError> {
        let header = self
            .headers()
            .get(name)
            .ok_or(ResponseError::missing_header(name))?
            .to_str()
            .map_err(|_| ResponseError::missing_header("X-Meili-API-Key"))?
            .to_string();
        Ok(header)
    }

    fn url_param(&self, name: &str) -> Result<String, ResponseError> {
        let param = self
            .param::<String>(name)
            .map_err(|e| ResponseError::bad_parameter(name, e))?;
        Ok(param)
    }

    fn index(&self) -> Result<Index, ResponseError> {
        let index_uid = self.url_param("index")?;
        let index = self
            .state()
            .db
            .open_index(&index_uid)
            .ok_or(ResponseError::index_not_found(index_uid))?;
        Ok(index)
    }

    fn identifier(&self) -> Result<String, ResponseError> {
        let name = self
            .param::<String>("identifier")
            .map_err(|e| ResponseError::bad_parameter("identifier", e))?;

        Ok(name)
    }
}
