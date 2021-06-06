use actix_web::HttpResponse;

pub fn markdown(md: &str) -> HttpResponse {
    HttpResponse::Ok()
        .content_type("text/html")
        .body(md_to_html(&md))
}

fn md_to_html(md: &str) -> String {
    let mut opt = comrak::ComrakOptions::default();
    opt.extension.table = true;
    opt.extension.autolink = true;
    comrak::markdown_to_html(md, &opt)
}
