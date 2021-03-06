use crate::hook::GeneralOutput;
use crate::mlmd::context::{Context, ContextOrderByField};
use crate::time::DateTime;
use crate::web::{response, Config};
use actix_web::{get, web, HttpResponse};
use std::collections::{HashMap, HashSet};
use std::time::Duration;

#[get("/contexts/{id}/contents/{name}")]
async fn get_context_content(
    config: web::Data<Config>,
    path: web::Path<(i32, String)>,
) -> actix_web::Result<HttpResponse> {
    let (id, content_name) = path.into_inner();

    let mut store = config.connect_metadata_store().await?;

    let contexts = store
        .get_contexts()
        .id(mlmd::metadata::ContextId::new(id))
        .execute()
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;
    if contexts.is_empty() {
        return Err(actix_web::error::ErrorNotFound(format!(
            "no such context: {}",
            id
        )));
    }

    let types = store
        .get_context_types()
        .id(contexts[0].type_id)
        .execute()
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;
    if contexts.is_empty() {
        return Err(actix_web::error::ErrorInternalServerError(format!(
            "no such context type: {}",
            contexts[0].type_id.get(),
        )));
    }
    let context = Context::from((types[0].clone(), contexts[0].clone()));

    let output = config
        .hook_runner
        .run_context_content_hook(context, &content_name)
        .await?;

    match output {
        GeneralOutput::Json(x) => Ok(response::json(&x)),
        GeneralOutput::Markdown(x) => Ok(response::markdown(&x)),
        GeneralOutput::Html(x) => Ok(response::html(&x)),
        GeneralOutput::Redirect(x) => Ok(response::redirect(&x)),
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct GetContextsQuery {
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub type_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<usize>,
    #[serde(default)]
    pub order_by: ContextOrderByField,
    #[serde(default)]
    pub asc: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mtime_start: Option<DateTime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mtime_end: Option<DateTime>,
}

impl GetContextsQuery {
    // TODO
    async fn get_contexts(
        &self,
        store: &mut mlmd::MetadataStore,
    ) -> anyhow::Result<Vec<mlmd::metadata::Context>> {
        let mut request = store.get_contexts().limit(self.limit.unwrap_or(100));
        if let Some(c) = self.execution {
            request = request.execution(mlmd::metadata::ExecutionId::new(c));
        }
        if let Some(c) = self.artifact {
            request = request.artifact(mlmd::metadata::ArtifactId::new(c));
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

        match (self.mtime_start, self.mtime_end) {
            (None, None) => {}
            (Some(start), None) => {
                request =
                    request.update_time(Duration::from_millis(start.timestamp_millis() as u64)..);
            }
            (None, Some(end)) => {
                request =
                    request.update_time(..Duration::from_millis(end.timestamp_millis() as u64));
            }
            (Some(start), Some(end)) => {
                request = request.update_time(
                    Duration::from_millis(start.timestamp_millis() as u64)
                        ..Duration::from_millis(end.timestamp_millis() as u64),
                );
            }
        }

        Ok(request.execute().await?)
    }

    async fn get_context_types(
        &self,
        store: &mut mlmd::MetadataStore,
        contexts: &[mlmd::metadata::Context],
    ) -> anyhow::Result<HashMap<mlmd::metadata::TypeId, mlmd::metadata::ContextType>> {
        let context_type_ids = contexts.iter().map(|x| x.type_id).collect::<HashSet<_>>();
        Ok(store
            .get_context_types()
            .ids(context_type_ids.into_iter())
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

    fn reset_mtime_start(&self) -> Self {
        let mut this = self.clone();
        this.mtime_start = None;
        this.offset = None;
        this
    }

    fn reset_mtime_end(&self) -> Self {
        let mut this = self.clone();
        this.mtime_end = None;
        this.offset = None;
        this
    }

    fn filter_type(&self, type_name: &str) -> Self {
        let mut this = self.clone();
        this.type_name = Some(type_name.to_owned());
        this.offset = None;
        this
    }

    fn order_by(&self, field: ContextOrderByField, asc: bool) -> Self {
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
            .map(|(k, v)| {
                format!(
                    "{}={}",
                    k,
                    v.to_string().trim_matches('"').replace('+', "%2B") // TODO: escape
                )
            })
            .collect::<Vec<_>>();
        format!("/contexts/?{}", qs.join("&"))
    }

    fn offset(&self) -> usize {
        self.offset.unwrap_or(0)
    }

    fn limit(&self) -> usize {
        self.limit.unwrap_or(100)
    }
}

#[get("/contexts/")]
pub async fn get_contexts(
    config: web::Data<Config>,
    query: web::Query<GetContextsQuery>,
) -> actix_web::Result<HttpResponse> {
    let mut store = config.connect_metadata_store().await?;

    let contexts = query
        .get_contexts(&mut store)
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;
    let context_types = query
        .get_context_types(&mut store, &contexts)
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;

    let mut md = "# Contexts\n".to_string();
    let mut pager_md = String::new();
    if query.offset() != 0 {
        pager_md += &format!(" [<<]({})", query.prev().to_url());
    } else {
        pager_md += " <<";
    }
    pager_md += &format!(
        " {}~{} ",
        query.offset() + 1,
        query.offset() + contexts.len()
    );
    if contexts.len() == query.limit() {
        pager_md += &format!("[>>]({})", query.next().to_url());
    } else {
        pager_md += ">>";
    }

    md += &pager_md;
    md += &format!(
        r#",
Update Time: <input type="date" id="start_date" {} onchange="filter_start_date()"> ~
             <input type="date" id="end_date" {} onchange="filter_end_date()">

<script type="text/javascript">
function filter_start_date() {{
  var v = document.getElementById("start_date").value;
  location.href = "{}&mtime-start=" + v + "T00:00:00%2B09:00";
}}
</script>
<script type="text/javascript">
function filter_end_date() {{
  var v = document.getElementById("end_date").value;
  location.href = "{}&mtime-end=" + v + "T00:00:00%2B09:00";
}}
</script>
"#,
        if let Some(v) = &query.mtime_start {
            format!("value={:?}", v.format("%Y-%m-%d").to_string())
        } else {
            "".to_owned()
        },
        if let Some(v) = &query.mtime_end {
            format!("value={:?}", v.format("%Y-%m-%d").to_string())
        } else {
            "".to_owned()
        },
        query.reset_mtime_start().to_url(),
        query.reset_mtime_end().to_url()
    );

    md += "\n";
    md += &format!(
        "| id{}{} | type | name{}{} | update-time{}{} | summary |\n",
        if query.order_by == ContextOrderByField::Id && query.asc {
            format!("<")
        } else {
            format!(
                "[<]({})",
                query.order_by(ContextOrderByField::Id, true).to_url()
            )
        },
        if query.order_by == ContextOrderByField::Id && !query.asc {
            format!(">")
        } else {
            format!(
                "[>]({})",
                query.order_by(ContextOrderByField::Id, false).to_url()
            )
        },
        if query.order_by == ContextOrderByField::Name && query.asc {
            format!("<")
        } else {
            format!(
                "[<]({})",
                query.order_by(ContextOrderByField::Name, true).to_url()
            )
        },
        if query.order_by == ContextOrderByField::Name && !query.asc {
            format!(">")
        } else {
            format!(
                "[>]({})",
                query.order_by(ContextOrderByField::Name, false).to_url()
            )
        },
        if query.order_by == ContextOrderByField::UpdateTime && query.asc {
            format!("<")
        } else {
            format!(
                "[<]({})",
                query
                    .order_by(ContextOrderByField::UpdateTime, true)
                    .to_url()
            )
        },
        if query.order_by == ContextOrderByField::UpdateTime && !query.asc {
            format!(">")
        } else {
            format!(
                "[>]({})",
                query
                    .order_by(ContextOrderByField::UpdateTime, false)
                    .to_url()
            )
        }
    );
    md += "|------|------|--------|-------|-------|\n";

    let contexts = contexts
        .into_iter()
        .map(|a| Context::from((context_types[&a.type_id].clone(), a)))
        .collect();
    let contexts = config
        .hook_runner
        .run_context_summary_hook(contexts)
        .await?;
    for a in contexts {
        md += &format!(
            "| [{}]({}) | [{}]({}) | {} | {} | {} |\n",
            a.id,
            format!("/contexts/{}", a.id),
            a.type_name,
            query.filter_type(&a.type_name).to_url(),
            a.name,
            a.mtime,
            a.summary.as_ref().map_or("", |x| x.as_str())
        );
    }

    md += "\n";
    md += &pager_md;
    Ok(response::markdown(&md))
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct GetContextQuery {
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub type_name: Option<String>,
}

#[get("/contexts/{id}")]
pub async fn get_context(
    config: web::Data<Config>,
    path: web::Path<(String,)>,
    query: web::Query<GetContextQuery>,
) -> actix_web::Result<HttpResponse> {
    let id_or_name = &path.0;
    let mut store = config.connect_metadata_store().await?;

    let contexts = match id_or_name.parse::<i32>().ok() {
        Some(id) => store
            .get_contexts()
            .id(mlmd::metadata::ContextId::new(id))
            .execute()
            .await
            .map_err(actix_web::error::ErrorInternalServerError)?,
        None => {
            let name = id_or_name;
            if let Some(type_name) = &query.type_name {
                store
                    .get_contexts()
                    .type_and_name(type_name, name)
                    .execute()
                    .await
                    .map_err(actix_web::error::ErrorInternalServerError)?
            } else {
                return Err(actix_web::error::ErrorBadRequest(format!(
                    "`type` query parameter must be specified"
                )));
            }
        }
    };
    if contexts.is_empty() {
        return Err(actix_web::error::ErrorNotFound(format!(
            "no such context: {}",
            id_or_name
        )));
    }

    let types = store
        .get_context_types()
        .id(contexts[0].type_id)
        .execute()
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;
    if contexts.is_empty() {
        return Err(actix_web::error::ErrorInternalServerError(format!(
            "no such context type: {}",
            contexts[0].type_id.get(),
        )));
    }

    let context = Context::from((types[0].clone(), contexts[0].clone()));
    let context = config.hook_runner.run_context_detail_hook(context).await?;

    let mut md = "# Context\n".to_string();

    md += &format!("- **ID**: {}\n", context.id);
    md += &format!(
        "- **Type**: [{}](/context_types/{})\n",
        context.type_name,
        types[0].id.get()
    );
    md += &format!("- **Name**: {}\n", context.name);
    md += &format!("- **Create Time**: {}\n", context.ctime);
    md += &format!("- **Update Time**: {}\n", context.mtime);

    if !context.properties.is_empty() {
        md += &format!("- **Properties**:\n");
        for (k, v) in &context.properties {
            md += &format!("  - **{}**: {}\n", k, v);
        }
    }
    if !context.custom_properties.is_empty() {
        md += &format!("- **Custom Properties**:\n");
        for (k, v) in &context.custom_properties {
            md += &format!("  - **{}**: {}\n", k, v);
        }
    }

    let associations_len = store
        .get_executions()
        .context(mlmd::metadata::ContextId::new(context.id))
        .count()
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;

    let attributions_len = store
        .get_artifacts()
        .context(mlmd::metadata::ContextId::new(context.id))
        .count()
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;
    if associations_len > 0 {
        md += &format!(
            "- [**Executions**](/executions/?context={}) ({})\n",
            context.id, associations_len
        );
    }
    if attributions_len > 0 {
        md += &format!(
            "- [**Artifacts**](/artifacts/?context={}) ({})\n",
            context.id, attributions_len
        );
    }

    Ok(response::markdown(&md))
}
