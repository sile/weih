use crate::hook::HookRunner;
use actix_web::{App, HttpServer};
use std::sync::Arc;

pub mod handlers;
pub mod link;
pub mod response;

#[derive(Debug, Clone)]
pub struct Config {
    mlmd_db: Arc<String>,
    hook_runner: Arc<HookRunner>,
}

impl Config {
    pub async fn connect_metadata_store(&self) -> actix_web::Result<mlmd::MetadataStore> {
        let store = mlmd::MetadataStore::connect(&self.mlmd_db)
            .await
            .map_err(actix_web::error::ErrorInternalServerError)?;
        Ok(store)
    }
}

pub async fn http_server_run(
    bind_addr: std::net::SocketAddr,
    mlmd_db: String,
    hook_runner: HookRunner,
) -> anyhow::Result<()> {
    let config = Config {
        mlmd_db: Arc::new(mlmd_db.to_owned()),
        hook_runner: Arc::new(hook_runner),
    };
    HttpServer::new(move || {
        App::new()
            .app_data(config.clone())
            .service(self::handlers::index::get_index)
            .service(self::handlers::artifact_types::get_artifact_type_summaries)
            .service(self::handlers::artifact_types::get_artifact_type_detail)
            .service(self::handlers::artifacts::get_artifacts)
            .service(self::handlers::artifacts::get_artifact)
            .service(self::handlers::artifacts::get_artifact_content)
            .service(self::handlers::execution_types::get_execution_type_summaries)
            .service(self::handlers::execution_types::get_execution_type_detail)
            .service(self::handlers::executions::get_executions)
            .service(self::handlers::executions::get_execution)
            .service(self::handlers::executions::get_execution_content)
            .service(self::handlers::context_types::get_context_type_summaries)
            .service(self::handlers::context_types::get_context_type_detail)
            .service(self::handlers::contexts::get_contexts)
            .service(self::handlers::contexts::get_context)
            .service(self::handlers::contexts::get_context_content)
            .service(self::handlers::events::get_events)
            .service(self::handlers::plot::plot_histogram)
            .service(self::handlers::plot::plot_scatter)
    })
    .bind(bind_addr)?
    .run()
    .await?;
    Ok(())
}
