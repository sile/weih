use crate::mlmd::artifact::{ArtifactTypeDetail, ArtifactTypeSummary};
use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Config {
    mlmd_db: Arc<String>,
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
) -> anyhow::Result<()> {
    let config = Config {
        mlmd_db: Arc::new(mlmd_db.to_owned()),
    };
    HttpServer::new(move || {
        App::new()
            .data(config.clone())
            .service(web::resource("/").route(web::get().to(index)))
            .service(web::resource("/artifact_types/").route(web::get().to(get_artifact_types)))
            .service(web::resource("/artifact_types/{id}").route(web::get().to(get_artifact_type)))
    })
    .bind(bind_addr)?
    .run()
    .await?;
    Ok(())
}

async fn index(_config: web::Data<Config>, _req: HttpRequest) -> impl Responder {
    let md = r#"
# ml-metadata web viewer

- [Artifacts](/artifacts/)
- [Artifact Types](/artifact_types/)
- [Executions](/executions/)
- [Executions Types](/execution_types/)
- [Contexts](/contexts/)
- [Context Typess](/context_types/)
- [Events](/events/)
"#;
    HttpResponse::Ok()
        .content_type("text/html")
        .body(md_to_html(&md))
}

async fn get_artifact_types(
    config: web::Data<Config>,
    _req: HttpRequest,
) -> actix_web::Result<HttpResponse> {
    let mut store = config.connect_metadata_store().await?;

    let types = store
        .get_artifact_types()
        .execute()
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;

    let mut md = r#"
# Artifact Types

| id | name | properties |
|----|------|------------|
"#
    .to_string();

    for ty in types {
        let ty = ArtifactTypeSummary::from(ty);
        md += &format!(
            "| [{}]({}) | {} | {:?} |\n",
            ty.id,
            format!("/artifact_types/{}", ty.id),
            ty.name,
            ty.properties
        );
    }

    Ok(HttpResponse::Ok()
        .content_type("text/html")
        .body(md_to_html(&md)))
}

async fn get_artifact_type(
    config: web::Data<Config>,
    path: web::Path<(i32,)>,
) -> actix_web::Result<HttpResponse> {
    let id = path.0;
    let mut store = config.connect_metadata_store().await?;

    let types = store
        .get_artifact_types()
        .id(mlmd::metadata::TypeId::new(id))
        .execute()
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;
    if types.is_empty() {
        return Err(actix_web::error::ErrorNotFound(format!(
            "no such artifact type: {}",
            id
        )));
    }
    let ty = ArtifactTypeDetail::from(types[0].clone());

    let mut md = "# Artifact Type\n".to_string();

    md += &format!("- ID: {}\n", ty.id);
    md += &format!("- Name: {}\n", ty.name);
    md += &format!("- Properties:\n");

    for (k, v) in &ty.properties {
        md += &format!("  - {}: {}\n", k, v);
    }
    md += &format!("- [Artifacts](/artifacts/?type={})\n", ty.name); // TODO: escape

    Ok(HttpResponse::Ok()
        .content_type("text/html")
        .body(md_to_html(&md)))
}

fn md_to_html(md: &str) -> String {
    let mut opt = comrak::ComrakOptions::default();
    opt.extension.table = true;
    opt.extension.autolink = true;
    comrak::markdown_to_html(md, &opt)
}
