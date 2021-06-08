use crate::hook::HookRunner;
use crate::mlmd::artifact::{ArtifactDetail, ArtifactOrderByField, ArtifactSummary};
use actix_web::{web, App, HttpResponse, HttpServer};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub mod handlers;
pub mod link;
pub mod response;

#[derive(Debug, Clone)]
pub struct Config {
    mlmd_db: Arc<String>,
    hook_runner: Arc<HookRunner>,
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
    hook_runner: HookRunner,
) -> anyhow::Result<()> {
    let config = Config {
        mlmd_db: Arc::new(mlmd_db.to_owned()),
        hook_runner: Arc::new(hook_runner),
    };
    HttpServer::new(move || {
        App::new()
            .data(config.clone())
            .service(self::handlers::index::get_index)
            .service(self::handlers::artifact_types::get_artifact_type_summaries)
            .service(self::handlers::artifact_types::get_artifact_type_detail)
            .service(self::handlers::execution_types::get_execution_type_summaries)
            .service(self::handlers::execution_types::get_execution_type_detail)
            .service(self::handlers::context_types::get_context_type_summaries)
            .service(self::handlers::context_types::get_context_type_detail)
            .service(web::resource("/artifacts/").route(web::get().to(get_artifacts)))
            .service(web::resource("/artifacts/{id}").route(web::get().to(get_artifact)))
            .service(self::handlers::events::get_events)
    })
    .bind(bind_addr)?
    .run()
    .await?;
    Ok(())
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct GetArtifactsQuery {
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub type_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
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
            if let Some(m) = &self.name {
                request = request.type_and_name(n, m);
            } else {
                request = request.ty(n);
            }
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
        md += " <<";
    }
    md += &format!(
        " {}~{} ",
        query.offset() + 1,
        query.offset() + artifacts.len()
    );
    if artifacts.len() == query.limit() {
        md += &format!("[>>]({})", query.next().to_url());
    } else {
        md += ">>";
    }

    md += "\n";
    md += &format!(
        "| id{}{} | type | name{}{} | state | update-time{}{} |\n",
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
    md += "|------|------|--------|-------|---------------|\n";

    for artifact in artifacts {
        let a = ArtifactSummary::from((artifact_types[&artifact.type_id].clone(), artifact));
        md += &format!(
            "| [{}]({}) | [{}]({}) | {} | {} | {} |\n",
            a.id,
            format!("/artifacts/{}", a.id),
            a.type_name,
            query.filter_type(&a.type_name).to_url(),
            a.name.as_ref().map_or("", |x| x.as_str()),
            a.state,
            a.mtime
        );
    }

    Ok(HttpResponse::Ok()
        .content_type("text/html")
        .body(md_to_html(&md)))
}

async fn get_artifact(
    config: web::Data<Config>,
    path: web::Path<(i32,)>,
) -> actix_web::Result<HttpResponse> {
    let id = path.0;
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

    let artifact = ArtifactDetail::from((types[0].clone(), artifacts[0].clone()));
    let artifact = config
        .hook_runner
        .run_artifact_detail_hook(artifact)
        .await?;

    let mut md = "# Artifact\n".to_string();

    md += &format!("- **ID**: {}\n", artifact.id);
    md += &format!(
        "- **Type**: [{}](/artifact_types/{})\n",
        artifact.type_name,
        types[0].id.get()
    );
    if let Some(x) = &artifact.name {
        md += &format!("- **Name**: {}\n", x);
    }
    if let Some(x) = &artifact.uri {
        md += &format!("- **URI**: {}\n", x);
    }
    md += &format!("- **State**: {}\n", artifact.state);
    md += &format!("- **Create Time**: {}\n", artifact.ctime);
    md += &format!("- **Update Time**: {}\n", artifact.mtime);

    if !artifact.properties.is_empty() {
        md += &format!("- **Properties**:\n");
        for (k, v) in &artifact.properties {
            md += &format!("  - **{}**: {}\n", k, v);
        }
    }
    if !artifact.custom_properties.is_empty() {
        md += &format!("- **Custom Properties**:\n");
        for (k, v) in &artifact.custom_properties {
            md += &format!("  - **{}**: {}\n", k, v);
        }
    }
    // TODO: Add count
    md += &format!("- [**Contexts**](/contexts/?artifact={})\n", artifact.id);
    md += &format!("- [**Events**](/events/?artifact={})\n", artifact.id);

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
