use crate::hook::GeneralOutput;
use crate::mlmd::artifact::Artifact;
use crate::web::{response, Config};
use actix_web::{get, web, HttpResponse};

#[get("/artifacts/{id}/contents/{name}")]
async fn get_artifact_content(
    config: web::Data<Config>,
    path: web::Path<(i32, String)>,
) -> actix_web::Result<HttpResponse> {
    let (id, content_name) = path.into_inner();

    let mut store = config.connect_metadata_store().await?;

    let artifacts = store
        .get_artifacts()
        .id(mlmd::metadata::ArtifactId::new(id))
        .execute()
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;
    if artifacts.is_empty() {
        return Err(actix_web::error::ErrorNotFound(format!(
            "no such artifact: {}",
            id
        )));
    }

    let types = store
        .get_artifact_types()
        .id(artifacts[0].type_id)
        .execute()
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;
    if artifacts.is_empty() {
        return Err(actix_web::error::ErrorInternalServerError(format!(
            "no such artifact tyep: {}",
            artifacts[0].type_id.get(),
        )));
    }
    let artifact = Artifact::from((types[0].clone(), artifacts[0].clone()));

    let output = config
        .hook_runner
        .run_artifact_content_hook(artifact, &content_name)
        .await?;

    match output {
        GeneralOutput::Json(x) => Ok(response::json(&x)),
        GeneralOutput::Markdown(x) => Ok(response::markdown(&x)),
        GeneralOutput::Html(x) => Ok(response::html(&x)),
    }
}
