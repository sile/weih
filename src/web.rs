use crate::mlmd::artifact::{
    ArtifactOrderByField, ArtifactSummary, ArtifactTypeDetail, ArtifactTypeSummary,
};
use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use std::collections::{HashMap, HashSet};
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
            .service(web::resource("/artifacts/").route(web::get().to(get_artifacts)))
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct GetArtifactsQuery {
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub type_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<usize>,
    #[serde(default)]
    pub order_by: ArtifactOrderByField,
    #[serde(default)]
    pub asc: bool,
}

impl GetArtifactsQuery {
    // TODO
    async fn get_artifacts(
        &self,
        store: &mut mlmd::MetadataStore,
    ) -> anyhow::Result<Vec<mlmd::metadata::Artifact>> {
        let context_id = if let Some(context) = self.context {
            Some(mlmd::metadata::ContextId::new(context))
        } else {
            None
        };

        let mut request = store.get_artifacts().limit(self.limit.unwrap_or(100));
        if let Some(c) = context_id {
            request = request.context(c)
        }
        if let Some(n) = self.offset {
            request = request.offset(n);
        }
        if let Some(n) = &self.type_name {
            request = request.ty(n);
        }
        request = request.order_by(self.order_by.into(), self.asc);

        Ok(request.execute().await?)
    }

    async fn get_artifact_types(
        &self,
        store: &mut mlmd::MetadataStore,
        artifacts: &[mlmd::metadata::Artifact],
    ) -> anyhow::Result<HashMap<mlmd::metadata::TypeId, mlmd::metadata::ArtifactType>> {
        let artifact_type_ids = artifacts.iter().map(|x| x.type_id).collect::<HashSet<_>>();
        Ok(store
            .get_artifact_types()
            .ids(artifact_type_ids.into_iter())
            .execute()
            .await?
            .into_iter()
            .map(|x| (x.id, x))
            .collect())
    }

    fn prev(&self) -> Self {
        let mut this = self.clone();
        this.offset = Some(
            self.offset
                .unwrap_or(0)
                .saturating_sub(self.limit.unwrap_or(100)),
        );
        this
    }

    fn next(&self) -> Self {
        let mut this = self.clone();
        this.offset = Some(self.offset() + self.limit());
        this
    }

    fn filter_type(&self, type_name: &str) -> Self {
        let mut this = self.clone();
        this.type_name = Some(type_name.to_owned());
        this.offset = None;
        this
    }

    fn order_by(&self, field: ArtifactOrderByField, asc: bool) -> Self {
        let mut this = self.clone();
        this.order_by = field;
        this.asc = asc;
        this.offset = None;
        this
    }

    fn to_url(&self) -> String {
        let qs = serde_json::to_value(self)
            .expect("unreachable")
            .as_object()
            .expect("unwrap")
            .into_iter()
            .map(|(k, v)| format!("{}={}", k, v.to_string().trim_matches('"')))
            .collect::<Vec<_>>();
        format!("/artifacts/?{}", qs.join("&"))
    }

    fn offset(&self) -> usize {
        self.offset.unwrap_or(0)
    }

    fn limit(&self) -> usize {
        self.limit.unwrap_or(100)
    }
}

async fn get_artifacts(
    config: web::Data<Config>,
    query: web::Query<GetArtifactsQuery>,
) -> actix_web::Result<HttpResponse> {
    let mut store = config.connect_metadata_store().await?;

    let artifacts = query
        .get_artifacts(&mut store)
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;
    let artifact_types = query
        .get_artifact_types(&mut store, &artifacts)
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;

    let mut md = "# Artifacts\n".to_string();

    if query.offset() != 0 {
        md += &format!(" [<<]({})", query.prev().to_url());
    } else {
        md += "<<";
    }
    if artifacts.len() == query.limit() {
        md += &format!(" [>>]({})", query.next().to_url());
    } else {
        md += ">>";
    }

    md += "\n";
    md += &format!(
        "| id{}{} | type | name{}{} | state | create-time{}{} | update-time{}{} |\n",
        if query.order_by == ArtifactOrderByField::Id && query.asc {
            format!("<")
        } else {
            format!(
                "[<]({})",
                query.order_by(ArtifactOrderByField::Id, true).to_url()
            )
        },
        if query.order_by == ArtifactOrderByField::Id && !query.asc {
            format!(">")
        } else {
            format!(
                "[>]({})",
                query.order_by(ArtifactOrderByField::Id, false).to_url()
            )
        },
        if query.order_by == ArtifactOrderByField::Name && query.asc {
            format!("<")
        } else {
            format!(
                "[<]({})",
                query.order_by(ArtifactOrderByField::Name, true).to_url()
            )
        },
        if query.order_by == ArtifactOrderByField::Name && !query.asc {
            format!(">")
        } else {
            format!(
                "[>]({})",
                query.order_by(ArtifactOrderByField::Name, false).to_url()
            )
        },
        if query.order_by == ArtifactOrderByField::CreateTime && query.asc {
            format!("<")
        } else {
            format!(
                "[<]({})",
                query
                    .order_by(ArtifactOrderByField::CreateTime, true)
                    .to_url()
            )
        },
        if query.order_by == ArtifactOrderByField::CreateTime && !query.asc {
            format!(">")
        } else {
            format!(
                "[>]({})",
                query
                    .order_by(ArtifactOrderByField::CreateTime, false)
                    .to_url()
            )
        },
        if query.order_by == ArtifactOrderByField::UpdateTime && query.asc {
            format!("<")
        } else {
            format!(
                "[<]({})",
                query
                    .order_by(ArtifactOrderByField::UpdateTime, true)
                    .to_url()
            )
        },
        if query.order_by == ArtifactOrderByField::UpdateTime && !query.asc {
            format!(">")
        } else {
            format!(
                "[>]({})",
                query
                    .order_by(ArtifactOrderByField::UpdateTime, false)
                    .to_url()
            )
        }
    );
    md += "|------|------|--------|-------|---------------|---------------|\n";

    for artifact in artifacts {
        let a = ArtifactSummary::from((artifact_types[&artifact.type_id].clone(), artifact));
        md += &format!(
            "| [{}]({}) | [{}]({}) | {} | {} | {} | {} |\n",
            a.id,
            format!("/artifacts/{}", a.id),
            a.type_name,
            query.filter_type(&a.type_name).to_url(),
            a.name.as_ref().map_or("", |x| x.as_str()),
            a.state,
            a.ctime,
            a.utime
        );
    }

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
