use crate::hook::GeneralOutput;
use crate::mlmd::execution::{Execution, ExecutionOrderByField};
use crate::web::{response, Config};
use actix_web::{get, web, HttpResponse};
use std::collections::{HashMap, HashSet};

#[get("/executions/{id}/contents/{name}")]
async fn get_execution_content(
    config: web::Data<Config>,
    path: web::Path<(i32, String)>,
) -> actix_web::Result<HttpResponse> {
    let (id, content_name) = path.into_inner();

    let mut store = config.connect_metadata_store().await?;

    let executions = store
        .get_executions()
        .id(mlmd::metadata::ExecutionId::new(id))
        .execute()
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;
    if executions.is_empty() {
        return Err(actix_web::error::ErrorNotFound(format!(
            "no such execution: {}",
            id
        )));
    }

    let types = store
        .get_execution_types()
        .id(executions[0].type_id)
        .execute()
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;
    if executions.is_empty() {
        return Err(actix_web::error::ErrorInternalServerError(format!(
            "no such execution tyep: {}",
            executions[0].type_id.get(),
        )));
    }
    let execution = Execution::from((types[0].clone(), executions[0].clone()));

    let output = config
        .hook_runner
        .run_execution_content_hook(execution, &content_name)
        .await?;

    match output {
        GeneralOutput::Json(x) => Ok(response::json(&x)),
        GeneralOutput::Markdown(x) => Ok(response::markdown(&x)),
        GeneralOutput::Html(x) => Ok(response::html(&x)),
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct GetExecutionsQuery {
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
    pub order_by: ExecutionOrderByField,
    #[serde(default)]
    pub asc: bool,
}

impl GetExecutionsQuery {
    // TODO
    async fn get_executions(
        &self,
        store: &mut mlmd::MetadataStore,
    ) -> anyhow::Result<Vec<mlmd::metadata::Execution>> {
        let context_id = if let Some(context) = self.context {
            Some(mlmd::metadata::ContextId::new(context))
        } else {
            None
        };

        let mut request = store.get_executions().limit(self.limit.unwrap_or(100));
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

    async fn get_execution_types(
        &self,
        store: &mut mlmd::MetadataStore,
        executions: &[mlmd::metadata::Execution],
    ) -> anyhow::Result<HashMap<mlmd::metadata::TypeId, mlmd::metadata::ExecutionType>> {
        let execution_type_ids = executions.iter().map(|x| x.type_id).collect::<HashSet<_>>();
        Ok(store
            .get_execution_types()
            .ids(execution_type_ids.into_iter())
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

    fn order_by(&self, field: ExecutionOrderByField, asc: bool) -> Self {
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
        format!("/executions/?{}", qs.join("&"))
    }

    fn offset(&self) -> usize {
        self.offset.unwrap_or(0)
    }

    fn limit(&self) -> usize {
        self.limit.unwrap_or(100)
    }
}

#[get("/executions/")]
pub async fn get_executions(
    config: web::Data<Config>,
    query: web::Query<GetExecutionsQuery>,
) -> actix_web::Result<HttpResponse> {
    let mut store = config.connect_metadata_store().await?;

    let executions = query
        .get_executions(&mut store)
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;
    let execution_types = query
        .get_execution_types(&mut store, &executions)
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;

    let mut md = "# Executions\n".to_string();

    if query.offset() != 0 {
        md += &format!(" [<<]({})", query.prev().to_url());
    } else {
        md += " <<";
    }
    md += &format!(
        " {}~{} ",
        query.offset() + 1,
        query.offset() + executions.len()
    );
    if executions.len() == query.limit() {
        md += &format!("[>>]({})", query.next().to_url());
    } else {
        md += ">>";
    }

    md += "\n";
    md += &format!(
        "| id{}{} | type | name{}{} | state | update-time{}{} | summary |\n",
        if query.order_by == ExecutionOrderByField::Id && query.asc {
            format!("<")
        } else {
            format!(
                "[<]({})",
                query.order_by(ExecutionOrderByField::Id, true).to_url()
            )
        },
        if query.order_by == ExecutionOrderByField::Id && !query.asc {
            format!(">")
        } else {
            format!(
                "[>]({})",
                query.order_by(ExecutionOrderByField::Id, false).to_url()
            )
        },
        if query.order_by == ExecutionOrderByField::Name && query.asc {
            format!("<")
        } else {
            format!(
                "[<]({})",
                query.order_by(ExecutionOrderByField::Name, true).to_url()
            )
        },
        if query.order_by == ExecutionOrderByField::Name && !query.asc {
            format!(">")
        } else {
            format!(
                "[>]({})",
                query.order_by(ExecutionOrderByField::Name, false).to_url()
            )
        },
        if query.order_by == ExecutionOrderByField::UpdateTime && query.asc {
            format!("<")
        } else {
            format!(
                "[<]({})",
                query
                    .order_by(ExecutionOrderByField::UpdateTime, true)
                    .to_url()
            )
        },
        if query.order_by == ExecutionOrderByField::UpdateTime && !query.asc {
            format!(">")
        } else {
            format!(
                "[>]({})",
                query
                    .order_by(ExecutionOrderByField::UpdateTime, false)
                    .to_url()
            )
        }
    );
    md += "|------|------|--------|-------|-------|--------|\n";

    let executions = executions
        .into_iter()
        .map(|a| Execution::from((execution_types[&a.type_id].clone(), a)))
        .collect();
    let executions = config
        .hook_runner
        .run_execution_summary_hook(executions)
        .await?;
    for a in executions {
        md += &format!(
            "| [{}]({}) | [{}]({}) | {} | {} | {} | {} |\n",
            a.id,
            format!("/executions/{}", a.id),
            a.type_name,
            query.filter_type(&a.type_name).to_url(),
            a.name.as_ref().map_or("", |x| x.as_str()),
            a.state,
            a.mtime,
            a.summary.as_ref().map_or("", |x| x.as_str())
        );
    }

    Ok(response::markdown(&md))
}

#[get("/executions/{id}")]
pub async fn get_execution(
    config: web::Data<Config>,
    path: web::Path<(i32,)>,
) -> actix_web::Result<HttpResponse> {
    let id = path.0;
    let mut store = config.connect_metadata_store().await?;

    let executions = store
        .get_executions()
        .id(mlmd::metadata::ExecutionId::new(id))
        .execute()
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;
    if executions.is_empty() {
        return Err(actix_web::error::ErrorNotFound(format!(
            "no such execution: {}",
            id
        )));
    }

    let types = store
        .get_execution_types()
        .id(executions[0].type_id)
        .execute()
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;
    if executions.is_empty() {
        return Err(actix_web::error::ErrorInternalServerError(format!(
            "no such execution tyep: {}",
            executions[0].type_id.get(),
        )));
    }

    let execution = Execution::from((types[0].clone(), executions[0].clone()));
    let execution = config
        .hook_runner
        .run_execution_detail_hook(execution)
        .await?;

    let mut md = "# Execution\n".to_string();

    md += &format!("- **ID**: {}\n", execution.id);
    md += &format!(
        "- **Type**: [{}](/execution_types/{})\n",
        execution.type_name,
        types[0].id.get()
    );
    if let Some(x) = &execution.name {
        md += &format!("- **Name**: {}\n", x);
    }
    md += &format!("- **State**: {}\n", execution.state);
    md += &format!("- **Create Time**: {}\n", execution.ctime);
    md += &format!("- **Update Time**: {}\n", execution.mtime);

    if !execution.properties.is_empty() {
        md += &format!("- **Properties**:\n");
        for (k, v) in &execution.properties {
            md += &format!("  - **{}**: {}\n", k, v);
        }
    }
    if !execution.custom_properties.is_empty() {
        md += &format!("- **Custom Properties**:\n");
        for (k, v) in &execution.custom_properties {
            md += &format!("  - **{}**: {}\n", k, v);
        }
    }

    let contexts_len = store
        .get_contexts()
        .execution(mlmd::metadata::ExecutionId::new(execution.id))
        .count()
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;

    let events_len = store
        .get_events()
        .execution(mlmd::metadata::ExecutionId::new(execution.id))
        .count()
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;
    if contexts_len > 0 {
        md += &format!(
            "- [**Contexts**](/contexts/?execution={}) ({})\n",
            execution.id, contexts_len
        );
    }
    if events_len > 0 {
        md += &format!(
            "- [**Events**](/events/?execution={}) ({})\n",
            execution.id, events_len
        );
    }

    Ok(response::markdown(&md))
}