use actix_web::HttpResponse;

pub fn markdown(md: &str) -> HttpResponse {
    let md = format!(
        "<link href='/css/github-markdown.css' rel='stylesheet' /><article class='markdown-body'>{}</article>",
        md_to_html(md)
    );
    html(&md)
}

pub fn css(s: &str) -> HttpResponse {
    HttpResponse::Ok()
        .content_type("text/css")
        .body(s.to_string())
}

pub fn html(s: &str) -> HttpResponse {
    HttpResponse::Ok()
        .content_type("text/html")
        .body(s.to_string())
}

pub fn json(s: &str) -> HttpResponse {
    HttpResponse::Ok()
        .content_type("application/json")
        .body(s.to_string())
}

pub fn svg(s: &str) -> HttpResponse {
    HttpResponse::Ok()
        .content_type("image/svg+xml")
        .body(s.to_string())
}

pub fn redirect(url: &str) -> HttpResponse {
    HttpResponse::build(actix_web::http::StatusCode::TEMPORARY_REDIRECT)
        .append_header(("Location", url))
        .finish()
}

fn md_to_html(md: &str) -> String {
    let mut opt = comrak::ComrakOptions::default();
    opt.extension.table = true;
    opt.extension.autolink = true;
    opt.render.unsafe_ = true;
    comrak::markdown_to_html(md, &opt)
}
