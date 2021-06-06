use crate::mlmd::context::{ContextTypeDetail, ContextTypeSummary};
use crate::web::link::Link;
use crate::web::{response, Config};
use actix_web::{get, web, HttpResponse};

#[get("/context_types/")]
async fn get_context_type_summaries(config: web::Data<Config>) -> actix_web::Result<HttpResponse> {
    let mut store = config.connect_metadata_store().await?;
    let types = store
        .get_context_types()
        .execute()
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;

    let mut md = concat!(
        "# Context Types\n",
        "\n",
        "| id | name | properties |\n",
        "|----|------|------------|\n"
    )
    .to_string();

    for ty in types {
        let ty = ContextTypeSummary::from(ty);
        md += &format!(
            "| {} | {} | {:?} |\n",
            Link::ContextType(ty.id),
            ty.name,
            ty.properties
        );
    }

    Ok(response::markdown(&md))
}

#[get("/context_types/{id}")]
async fn get_context_type_detail(
    config: web::Data<Config>,
    path: web::Path<(i32,)>,
) -> actix_web::Result<HttpResponse> {
    let id = path.0;
    let mut store = config.connect_metadata_store().await?;

    let types = store
        .get_context_types()
        .id(mlmd::metadata::TypeId::new(id))
        .execute()
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;
    if types.is_empty() {
        return Err(actix_web::error::ErrorNotFound(format!(
            "no such context type: {}",
            id
        )));
    }
    let ty = ContextTypeDetail::from(types[0].clone());

    let mut md = "# Context Type\n".to_string();

    md += &format!("- ID: {}\n", ty.id);
    md += &format!("- Name: {}\n", ty.name);
    md += &format!("- Properties:\n");

    for (k, v) in &ty.properties {
        md += &format!("  - {}: {}\n", k, v);
    }
    md += &format!("- [Contexts](/contexts/?type={})\n", ty.name); // TODO: escape

    Ok(response::markdown(&md))
}
