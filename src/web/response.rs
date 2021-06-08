use actix_web::HttpResponse;

pub fn markdown(md: &str) -> HttpResponse {
    html(&md_to_html(&md))
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

fn md_to_html(md: &str) -> String {
    let mut opt = comrak::ComrakOptions::default();
    opt.extension.table = true;
    opt.extension.autolink = true;
    comrak::markdown_to_html(md, &opt)
}
