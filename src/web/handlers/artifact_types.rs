use crate::mlmd::artifact::{ArtifactTypeDetail, ArtifactTypeSummary};
use crate::web::link::Link;
use crate::web::{response, Config};
use actix_web::{get, web, HttpResponse};

#[get("/artifact_types/")]
async fn get_artifact_type_summaries(config: web::Data<Config>) -> actix_web::Result<HttpResponse> {
    let mut store = config.connect_metadata_store().await?;
    let types = store
        .get_artifact_types()
        .execute()
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;

    let mut md = concat!(
        "# Artifact Types\n",
        "\n",
        "| id | name | properties |\n",
        "|----|------|------------|\n"
    )
    .to_string();

    for ty in types {
        let ty = ArtifactTypeSummary::from(ty);
        md += &format!(
            "| {} | {} | {:?} |\n",
            Link::ArtifactType(ty.id),
            ty.name,
            ty.properties
        );
    }

    Ok(response::markdown(&md))
}

#[get("/artifact_types/{id}")]
async fn get_artifact_type_detail(
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

    Ok(response::markdown(&md))
}
