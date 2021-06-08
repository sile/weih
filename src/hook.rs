use crate::cli::hook::HookOpt;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ItemType {
    Artifact,
    Execution,
    Context,
}

impl std::str::FromStr for ItemType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s {
            "artifact" => Ok(Self::Artifact),
            "execution" => Ok(Self::Execution),
            "context" => Ok(Self::Context),
            _ => anyhow::bail!("unknown item type: {:?}", s),
        }
    }
}

impl ItemType {
    pub const POSSIBLE_VALUES: &'static [&'static str] = &["artifact", "execution", "context"];
}

#[derive(Debug)]
pub struct HookRunner {
    hooks: HashMap<HookKey, HookCommand>,
}

impl HookRunner {
    pub fn new(hook_opts: &[HookOpt], metadata_store_uri: &str) -> Self {
        let mut hooks = HashMap::new();
        for opt in hook_opts {
            let key = HookKey {
                item_type: opt.item_type,
                type_name: opt.type_name.clone(),
            };
            let val = HookCommand {
                path: opt.command.clone(),
                args: opt.args.clone(),
                envs: vec![("WEIH_MLMD_DB".to_string(), metadata_store_uri.to_string())]
                    .into_iter()
                    .collect(),
            };
            hooks.insert(key, val);
        }
        Self { hooks }
    }

    pub async fn run(&self, input: HookInput) -> actix_web::error::Result<Option<HookOutput>> {
        let item_type = input.item_type();
        let type_name = input.type_name().to_owned();
        let key = HookKey {
            item_type,
            type_name,
        };

        if let Some(command) = self.hooks.get(&key).cloned() {
            let output = actix_web::web::block(move || command.run(input))
                .await
                .map_err(actix_web::error::ErrorInternalServerError)?
                .map_err(actix_web::error::ErrorInternalServerError)?;
            Ok(Some(output))
        } else {
            Ok(None)
        }
    }

    pub async fn run_artifact_summary_hook(
        &self,
        artifacts: Vec<crate::mlmd::artifact::Artifact>,
    ) -> actix_web::error::Result<Vec<crate::mlmd::artifact::Artifact>> {
        let id_to_index = artifacts
            .iter()
            .enumerate()
            .map(|(index, a)| (a.id, index))
            .collect::<HashMap<_, _>>();
        let mut type_to_artifacts: HashMap<_, Vec<_>> = HashMap::new();
        for a in &artifacts {
            type_to_artifacts
                .entry(&a.type_name)
                .or_default()
                .push(a.clone());
        }

        let mut result = Vec::new();
        for (_, a) in type_to_artifacts {
            let input = HookInput::ArtifactSummary(a.clone());
            match self.run(input).await? {
                None => {
                    result.extend(a);
                }
                Some(HookOutput::ArtifactSummary(a)) => {
                    result.extend(a);
                }
                Some(o) => {
                    return Err(actix_web::error::ErrorInternalServerError(format!(
                        "unexpected hook result: {:?}",
                        o
                    )))
                }
            }
        }
        result.sort_by_key(|a| id_to_index[&a.id]);
        Ok(result)
    }

    pub async fn run_artifact_detail_hook(
        &self,
        artifact: crate::mlmd::artifact::Artifact,
    ) -> actix_web::error::Result<crate::mlmd::artifact::Artifact> {
        let input = HookInput::ArtifactDetail(ArtifactDetailHookInput {
            artifact: artifact.clone(),
        });
        match self.run(input).await? {
            None => Ok(artifact),
            Some(HookOutput::ArtifactDetail(o)) => Ok(o.artifact),
            Some(o) => Err(actix_web::error::ErrorInternalServerError(format!(
                "unexpected hook result: {:?}",
                o
            ))),
        }
    }

    pub async fn run_artifact_content_hook(
        &self,
        artifact: crate::mlmd::artifact::Artifact,
        content_name: &str,
    ) -> actix_web::error::Result<crate::hook::GeneralOutput> {
        let input = HookInput::ArtifactContent(ArtifactContentHookInput {
            artifact: artifact.clone(),
            content_name: content_name.to_owned(),
        });
        match self.run(input).await? {
            None => Err(actix_web::error::ErrorNotFound(format!(
                "no such content: {:?}",
                content_name
            ))),
            Some(HookOutput::ArtifactContent(o)) => Ok(o),
            Some(o) => Err(actix_web::error::ErrorInternalServerError(format!(
                "unexpected hook result: {:?}",
                o
            ))),
        }
    }

    pub async fn run_execution_summary_hook(
        &self,
        executions: Vec<crate::mlmd::execution::Execution>,
    ) -> actix_web::error::Result<Vec<crate::mlmd::execution::Execution>> {
        let id_to_index = executions
            .iter()
            .enumerate()
            .map(|(index, a)| (a.id, index))
            .collect::<HashMap<_, _>>();
        let mut type_to_executions: HashMap<_, Vec<_>> = HashMap::new();
        for a in &executions {
            type_to_executions
                .entry(&a.type_name)
                .or_default()
                .push(a.clone());
        }

        let mut result = Vec::new();
        for (_, a) in type_to_executions {
            let input = HookInput::ExecutionSummary(a.clone());
            match self.run(input).await? {
                None => {
                    result.extend(a);
                }
                Some(HookOutput::ExecutionSummary(a)) => {
                    result.extend(a);
                }
                Some(o) => {
                    return Err(actix_web::error::ErrorInternalServerError(format!(
                        "unexpected hook result: {:?}",
                        o
                    )))
                }
            }
        }
        result.sort_by_key(|a| id_to_index[&a.id]);
        Ok(result)
    }

    pub async fn run_execution_detail_hook(
        &self,
        execution: crate::mlmd::execution::Execution,
    ) -> actix_web::error::Result<crate::mlmd::execution::Execution> {
        let input = HookInput::ExecutionDetail(ExecutionDetailHookInput {
            execution: execution.clone(),
        });
        match self.run(input).await? {
            None => Ok(execution),
            Some(HookOutput::ExecutionDetail(o)) => Ok(o.execution),
            Some(o) => Err(actix_web::error::ErrorInternalServerError(format!(
                "unexpected hook result: {:?}",
                o
            ))),
        }
    }

    pub async fn run_execution_content_hook(
        &self,
        execution: crate::mlmd::execution::Execution,
        content_name: &str,
    ) -> actix_web::error::Result<crate::hook::GeneralOutput> {
        let input = HookInput::ExecutionContent(ExecutionContentHookInput {
            execution: execution.clone(),
            content_name: content_name.to_owned(),
        });
        match self.run(input).await? {
            None => Err(actix_web::error::ErrorNotFound(format!(
                "no such content: {:?}",
                content_name
            ))),
            Some(HookOutput::ExecutionContent(o)) => Ok(o),
            Some(o) => Err(actix_web::error::ErrorInternalServerError(format!(
                "unexpected hook result: {:?}",
                o
            ))),
        }
    }

    pub async fn run_context_summary_hook(
        &self,
        contexts: Vec<crate::mlmd::context::Context>,
    ) -> actix_web::error::Result<Vec<crate::mlmd::context::Context>> {
        let id_to_index = contexts
            .iter()
            .enumerate()
            .map(|(index, a)| (a.id, index))
            .collect::<HashMap<_, _>>();
        let mut type_to_contexts: HashMap<_, Vec<_>> = HashMap::new();
        for a in &contexts {
            type_to_contexts
                .entry(&a.type_name)
                .or_default()
                .push(a.clone());
        }

        let mut result = Vec::new();
        for (_, a) in type_to_contexts {
            let input = HookInput::ContextSummary(a.clone());
            match self.run(input).await? {
                None => {
                    result.extend(a);
                }
                Some(HookOutput::ContextSummary(a)) => {
                    result.extend(a);
                }
                Some(o) => {
                    return Err(actix_web::error::ErrorInternalServerError(format!(
                        "unexpected hook result: {:?}",
                        o
                    )))
                }
            }
        }
        result.sort_by_key(|a| id_to_index[&a.id]);
        Ok(result)
    }

    pub async fn run_context_detail_hook(
        &self,
        context: crate::mlmd::context::Context,
    ) -> actix_web::error::Result<crate::mlmd::context::Context> {
        let input = HookInput::ContextDetail(ContextDetailHookInput {
            context: context.clone(),
        });
        match self.run(input).await? {
            None => Ok(context),
            Some(HookOutput::ContextDetail(o)) => Ok(o.context),
            Some(o) => Err(actix_web::error::ErrorInternalServerError(format!(
                "unexpected hook result: {:?}",
                o
            ))),
        }
    }

    pub async fn run_context_content_hook(
        &self,
        context: crate::mlmd::context::Context,
        content_name: &str,
    ) -> actix_web::error::Result<crate::hook::GeneralOutput> {
        let input = HookInput::ContextContent(ContextContentHookInput {
            context: context.clone(),
            content_name: content_name.to_owned(),
        });
        match self.run(input).await? {
            None => Err(actix_web::error::ErrorNotFound(format!(
                "no such content: {:?}",
                content_name
            ))),
            Some(HookOutput::ContextContent(o)) => Ok(o),
            Some(o) => Err(actix_web::error::ErrorInternalServerError(format!(
                "unexpected hook result: {:?}",
                o
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HookKey {
    item_type: ItemType,
    type_name: String,
}

#[derive(Debug, Clone)]
pub struct HookCommand {
    path: PathBuf,
    args: Vec<String>,
    envs: HashMap<String, String>,
}

impl HookCommand {
    pub fn run(&self, input: HookInput) -> anyhow::Result<HookOutput> {
        let mut command = std::process::Command::new(&self.path);
        for (k, v) in &self.envs {
            command.env(k, v);
        }

        let mut child = command
            .args(&self.args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()?;

        serde_json::to_writer(child.stdin.as_mut().expect("unreachable"), &input)?;
        let _ = child.stdin.take();

        let output = child.wait_with_output()?;
        if output.status.success() {
            Ok(serde_json::from_slice(&output.stdout)?)
        } else {
            anyhow::bail!(
                "failed to execute hook command: status={}, stderr={}",
                output.status,
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum HookInput {
    ArtifactSummary(Vec<crate::mlmd::artifact::Artifact>),
    ArtifactDetail(ArtifactDetailHookInput),
    ArtifactContent(ArtifactContentHookInput),
    ExecutionSummary(Vec<crate::mlmd::execution::Execution>),
    ExecutionDetail(ExecutionDetailHookInput),
    ExecutionContent(ExecutionContentHookInput),
    ContextSummary(Vec<crate::mlmd::context::Context>),
    ContextDetail(ContextDetailHookInput),
    ContextContent(ContextContentHookInput),
}

impl HookInput {
    pub fn item_type(&self) -> ItemType {
        match self {
            Self::ArtifactSummary(_) | Self::ArtifactDetail(_) | Self::ArtifactContent(_) => {
                ItemType::Artifact
            }
            Self::ExecutionSummary(_) | Self::ExecutionDetail(_) | Self::ExecutionContent(_) => {
                ItemType::Execution
            }
            Self::ContextSummary(_) | Self::ContextDetail(_) | Self::ContextContent(_) => {
                ItemType::Context
            }
        }
    }

    pub fn type_name(&self) -> &str {
        match self {
            Self::ArtifactSummary(x) => &x[0].type_name,
            Self::ArtifactDetail(x) => &x.artifact.type_name,
            Self::ArtifactContent(x) => &x.artifact.type_name,
            Self::ExecutionSummary(x) => &x[0].type_name,
            Self::ExecutionDetail(x) => &x.execution.type_name,
            Self::ExecutionContent(x) => &x.execution.type_name,
            Self::ContextSummary(x) => &x[0].type_name,
            Self::ContextDetail(x) => &x.context.type_name,
            Self::ContextContent(x) => &x.context.type_name,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ArtifactDetailHookInput {
    pub artifact: crate::mlmd::artifact::Artifact,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ArtifactContentHookInput {
    pub artifact: crate::mlmd::artifact::Artifact,
    pub content_name: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ExecutionDetailHookInput {
    pub execution: crate::mlmd::execution::Execution,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ExecutionContentHookInput {
    pub execution: crate::mlmd::execution::Execution,
    pub content_name: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ContextDetailHookInput {
    pub context: crate::mlmd::context::Context,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ContextContentHookInput {
    pub context: crate::mlmd::context::Context,
    pub content_name: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum HookOutput {
    ArtifactSummary(Vec<crate::mlmd::artifact::Artifact>),
    ArtifactDetail(ArtifactDetailHookOutput),
    ArtifactContent(GeneralOutput),
    ExecutionSummary(Vec<crate::mlmd::execution::Execution>),
    ExecutionDetail(ExecutionDetailHookOutput),
    ExecutionContent(GeneralOutput),
    ContextSummary(Vec<crate::mlmd::context::Context>),
    ContextDetail(ContextDetailHookOutput),
    ContextContent(GeneralOutput),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ArtifactDetailHookOutput {
    pub artifact: crate::mlmd::artifact::Artifact,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ExecutionDetailHookOutput {
    pub execution: crate::mlmd::execution::Execution,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ContextDetailHookOutput {
    pub context: crate::mlmd::context::Context,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GeneralOutput {
    Json(String),
    Markdown(String),
    Html(String),
}
