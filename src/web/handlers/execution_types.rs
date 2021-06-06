use crate::mlmd::execution::{ExecutionTypeDetail, ExecutionTypeSummary};
use crate::web::link::Link;
use crate::web::{response, Config};
use actix_web::{get, web, HttpResponse};

#[get("/execution_types/")]
async fn get_execution_type_summaries(
    config: web::Data<Config>,
) -> actix_web::Result<HttpResponse> {
    let mut store = config.connect_metadata_store().await?;
    let types = store
        .get_execution_types()
        .execute()
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;

    let mut md = concat!(
        "# Execution Types\n",
        "\n",
        "| id | name | properties |\n",
        "|----|------|------------|\n"
    )
    .to_string();

    for ty in types {
        let ty = ExecutionTypeSummary::from(ty);
        md += &format!(
            "| {} | {} | {:?} |\n",
            Link::ExecutionType(ty.id),
            ty.name,
            ty.properties
        );
    }

    Ok(response::markdown(&md))
}

#[get("/execution_types/{id}")]
async fn get_execution_type_detail(
    config: web::Data<Config>,
    path: web::Path<(i32,)>,
) -> actix_web::Result<HttpResponse> {
    let id = path.0;
    let mut store = config.connect_metadata_store().await?;

    let types = store
        .get_execution_types()
        .id(mlmd::metadata::TypeId::new(id))
        .execute()
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;
    if types.is_empty() {
        return Err(actix_web::error::ErrorNotFound(format!(
            "no such execution type: {}",
            id
        )));
    }
    let ty = ExecutionTypeDetail::from(types[0].clone());

    let mut md = "# Execution Type\n".to_string();

    md += &format!("- ID: {}\n", ty.id);
    md += &format!("- Name: {}\n", ty.name);
    md += &format!("- Properties:\n");

    for (k, v) in &ty.properties {
        md += &format!("  - {}: {}\n", k, v);
    }
    md += &format!("- [Executions](/executions/?type={})\n", ty.name); // TODO: escape

    Ok(response::markdown(&md))
}
