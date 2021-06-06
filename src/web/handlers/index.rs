use crate::web::{response, Config};
use actix_web::{get, web, HttpResponse};

#[get("/")]
async fn get_index(_config: web::Data<Config>) -> actix_web::Result<HttpResponse> {
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
    Ok(response::markdown(md))
}
