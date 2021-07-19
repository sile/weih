use crate::web::response;
use actix_web::{get, HttpResponse};

#[get("/css/github-markdown.css")]
async fn get_github_markdown_css() -> actix_web::Result<HttpResponse> {
    Ok(response::css(include_str!(
        "../../../css/github-markdown.css"
    )))
}
