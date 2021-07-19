use crate::hook::GeneralOutput;
use crate::mlmd::artifact::{Artifact, ArtifactOrderByField};
use crate::time::DateTime;
use crate::web::{response, Config};
use actix_web::{get, web, HttpResponse};
use std::collections::{HashMap, HashSet};
use std::time::Duration;

#[get("/artifacts/{id}/contents/{name}")]
async fn get_artifact_content(
    config: web::Data<Config>,
    path: web::Path<(i32, String)>,
) -> actix_web::Result<HttpResponse> {
    let (id, content_name) = path.into_inner();

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
            "no such artifact type: {}",
            artifacts[0].type_id.get(),
        )));
    }
    let artifact = Artifact::from((types[0].clone(), artifacts[0].clone()));

    let output = config
        .hook_runner
        .run_artifact_content_hook(artifact, &content_name)
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mtime_start: Option<DateTime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mtime_end: Option<DateTime>,
}

impl GetArtifactsQuery {
    // TODO
    pub async fn get_artifacts(
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

    fn reset_mtime_start(&self) -> Self {
        let mut this = self.clone();
        this.mtime_start = None;
        this
    }

    fn reset_mtime_end(&self) -> Self {
        let mut this = self.clone();
        this.mtime_end = None;
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

    pub fn to_url(&self) -> String {
        format!("/artifacts/?{}", self.to_qs())
    }

    pub fn to_qs(&self) -> String {
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
        qs.join("&")
    }

    fn offset(&self) -> usize {
        self.offset.unwrap_or(0)
    }

    fn limit(&self) -> usize {
        self.limit.unwrap_or(100)
    }
}

#[get("/artifacts/")]
pub async fn get_artifacts(
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

    let mut pager_md = String::new();
    if query.offset() != 0 {
        pager_md += &format!(" [<<]({})", query.prev().to_url());
    } else {
        pager_md += " <<";
    }
    pager_md += &format!(
        " {}~{} ",
        query.offset() + 1,
        query.offset() + artifacts.len()
    );
    if artifacts.len() == query.limit() {
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
        "| id{}{} | type | name{}{} | state | update-time{}{} | summary |\n",
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
    md += "|------|------|--------|-------|-------|--------|\n";

    let artifacts = artifacts
        .into_iter()
        .map(|a| Artifact::from((artifact_types[&a.type_id].clone(), a)))
        .collect();
    let artifacts = config
        .hook_runner
        .run_artifact_summary_hook(artifacts)
        .await?;
    for a in artifacts {
        md += &format!(
            "| [{}]({}) | [{}]({}) | {} | {} | {} | {} |\n",
            a.id,
            format!("/artifacts/{}", a.id),
            a.type_name,
            query.filter_type(&a.type_name).to_url(),
            a.name.as_ref().map_or("", |x| x.as_str()),
            a.state,
            a.mtime,
            a.summary.as_ref().map_or("", |x| x.as_str())
        );
    }

    md += &pager_md;

    Ok(response::markdown(&md))
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct GetArtifactQuery {
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub type_name: Option<String>,
}

#[get("/artifacts/{id}")]
pub async fn get_artifact(
    config: web::Data<Config>,
    path: web::Path<(String,)>,
    query: web::Query<GetArtifactQuery>,
) -> actix_web::Result<HttpResponse> {
    let id_or_name = &path.0;
    let mut store = config.connect_metadata_store().await?;

    let artifacts = match id_or_name.parse::<i32>().ok() {
        Some(id) => store
            .get_artifacts()
            .id(mlmd::metadata::ArtifactId::new(id))
            .execute()
            .await
            .map_err(actix_web::error::ErrorInternalServerError)?,
        None => {
            let name = id_or_name;
            if let Some(type_name) = &query.type_name {
                store
                    .get_artifacts()
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
    if artifacts.is_empty() {
        return Err(actix_web::error::ErrorNotFound(format!(
            "no such artifact: {:?}",
            id_or_name
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
            "no such artifact type: {}",
            artifacts[0].type_id.get(),
        )));
    }

    let artifact = Artifact::from((types[0].clone(), artifacts[0].clone()));
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
    if !artifact.extra_properties.is_empty() {
        md += &format!("- **Extra Properties**:\n");
        for (k, v) in &artifact.extra_properties {
            md += &format!("  - **{}**: {}\n", k, v);
        }
    }

    let contexts_len = store
        .get_contexts()
        .artifact(mlmd::metadata::ArtifactId::new(artifact.id))
        .count()
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;

    let events_len = store
        .get_events()
        .artifact(mlmd::metadata::ArtifactId::new(artifact.id))
        .count()
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;
    if contexts_len > 0 {
        md += &format!(
            "- [**Contexts**](/contexts/?artifact={}) ({})\n",
            artifact.id, contexts_len
        );
    }
    if events_len > 0 {
        md += &format!(
            "- [**Events**](/events/?artifact={}) ({})\n",
            artifact.id, events_len
        );
    }

    md += &format!("- [**Graph**](/artifacts/{}/graph)\n", artifact.id);

    Ok(response::markdown(&md))
}

#[get("/artifacts/{id}/graph")]
pub async fn get_artifact_graph(
    config: web::Data<Config>,
    path: web::Path<(i32,)>,
) -> actix_web::Result<HttpResponse> {
    let id = path.0;
    let mut store = config.connect_metadata_store().await?;

    let graph = Graph::new(&mut store, id)
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;

    let svg = std::process::Command::new("dot")
        .arg("-Tsvg")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .ok()
        .and_then(|mut child| {
            use std::io::Write;

            let writer = child.stdin.as_mut()?;
            graph.render(writer).ok()?;
            writer.flush().ok()?;
            let output = child.wait_with_output().ok()?;
            if !output.status.success() {
                None
            } else {
                Some(output.stdout)
            }
        });

    if let Some(svg) = svg {
        Ok(response::svg(&String::from_utf8(svg).expect("TODO")))
    } else {
        let mut buf = Vec::new();
        graph.render(&mut buf).expect("TODO");
        Ok(response::markdown(&String::from_utf8(buf).expect("TODO")))
    }
}

use crate::web::handlers::executions::{Edge, Node, NodeId};

#[derive(Debug)]
struct Graph {
    nodes: Vec<Node>,
    edges: Vec<Edge>,
}

impl Graph {
    async fn new(store: &mut mlmd::MetadataStore, artifact_id: i32) -> anyhow::Result<Self> {
        let mut nodes = HashMap::new();
        let mut edges = Vec::new();
        let mut stack = vec![NodeId::Artifact(artifact_id)];
        while let Some(curr) = stack.pop() {
            if nodes.contains_key(&curr) {
                continue;
            }
            let mut curr = match curr {
                NodeId::Execution(id) => Node::Execution {
                    node: fetch_execution(store, id).await?,
                    inputs: 0,
                    outputs: 0,
                },
                NodeId::Artifact(id) => Node::Artifact {
                    node: fetch_artifact(store, id).await?,
                    inputs: 0,
                    outputs: 0,
                },
            };

            let events = match &curr {
                Node::Execution { node, .. } => {
                    store
                        .get_events()
                        .execution(mlmd::metadata::ExecutionId::new(node.id))
                        .execute()
                        .await?
                }
                Node::Artifact { node, .. } => {
                    store
                        .get_events()
                        .artifact(mlmd::metadata::ArtifactId::new(node.id))
                        .execute()
                        .await?
                }
            };
            curr.set_in_out(&events);
            nodes.insert(curr.id(), curr.clone());
            anyhow::ensure!(
                nodes.len() < 100,
                "Too many executions and artifact to visualize"
            );

            for event in events {
                if matches!(curr, Node::Artifact { .. }) {
                    use mlmd::metadata::EventType::*;
                    if event.artifact_id.get() == artifact_id
                        || matches!(event.ty, Output | DeclaredOutput | InternalOutput)
                    {
                        let id = NodeId::Execution(event.execution_id.get());
                        stack.push(id);
                        if matches!(event.ty, Output | DeclaredOutput | InternalOutput) {
                            edges.push(Edge {
                                source: id,
                                target: curr.id(),
                                event: event.into(),
                            });
                        }
                    }
                } else {
                    use mlmd::metadata::EventType::*;
                    if matches!(event.ty, Input | DeclaredInput | InternalInput) {
                        let id = NodeId::Artifact(event.artifact_id.get());
                        stack.push(id);
                        edges.push(Edge {
                            source: id,
                            target: curr.id(),
                            event: event.into(),
                        });
                    }
                }
            }
        }

        Ok(Self {
            nodes: nodes.into_iter().map(|x| x.1).collect(),
            edges,
        })
    }

    fn render<W: std::io::Write>(&self, writer: &mut W) -> anyhow::Result<()> {
        writeln!(writer, "digraph execution_graph {{")?;

        for node in &self.nodes {
            writeln!(writer, "{}[{}]", node.id(), node.attrs().join(","))?;
        }

        for edge in &self.edges {
            writeln!(
                writer,
                "{} -> {} [label={:?}];",
                edge.source,
                edge.target,
                format!("{:?}:{:?}", edge.event.ty, edge.event.path)
            )?;
        }

        writeln!(writer, "}}")?;
        Ok(())
    }
}

async fn fetch_execution(
    store: &mut mlmd::MetadataStore,
    id: i32,
) -> anyhow::Result<crate::mlmd::execution::Execution> {
    let executions = store
        .get_executions()
        .id(mlmd::metadata::ExecutionId::new(id))
        .execute()
        .await?;
    anyhow::ensure!(!executions.is_empty(), "no such execution: {}", id);

    let types = store
        .get_execution_types()
        .id(executions[0].type_id)
        .execute()
        .await?;
    anyhow::ensure!(
        !executions.is_empty(),
        "no such execution tyep: {}",
        executions[0].type_id.get()
    );

    Ok(crate::mlmd::execution::Execution::from((
        types[0].clone(),
        executions[0].clone(),
    )))
}

async fn fetch_artifact(
    store: &mut mlmd::MetadataStore,
    id: i32,
) -> anyhow::Result<crate::mlmd::artifact::Artifact> {
    let artifacts = store
        .get_artifacts()
        .id(mlmd::metadata::ArtifactId::new(id))
        .execute()
        .await?;
    anyhow::ensure!(!artifacts.is_empty(), "no such artifact: {}", id);

    let types = store
        .get_artifact_types()
        .id(artifacts[0].type_id)
        .execute()
        .await?;
    anyhow::ensure!(
        !artifacts.is_empty(),
        "no such artifact tyep: {}",
        artifacts[0].type_id.get()
    );

    Ok(crate::mlmd::artifact::Artifact::from((
        types[0].clone(),
        artifacts[0].clone(),
    )))
}
