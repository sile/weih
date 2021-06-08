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
    pub fn new(hook_opts: &[HookOpt]) -> Self {
        let mut hooks = HashMap::new();
        for opt in hook_opts {
            let key = HookKey {
                item_type: opt.item_type,
                type_name: opt.type_name.clone(),
            };
            let val = HookCommand {
                path: opt.command.clone(),
                args: opt.args.clone(),
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
}

impl HookCommand {
    pub fn run(&self, input: HookInput) -> anyhow::Result<HookOutput> {
        let mut child = std::process::Command::new(&self.path)
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
    ExecutionSummary,
    ExecutionDetail,
    ExecutionContent,
    ContextSummary,
    ContextDetail,
    ContextContent,
}

impl HookInput {
    pub fn item_type(&self) -> ItemType {
        match self {
            Self::ArtifactSummary(_) | Self::ArtifactDetail(_) | Self::ArtifactContent(_) => {
                ItemType::Artifact
            }
            Self::ExecutionSummary | Self::ExecutionDetail | Self::ExecutionContent => {
                ItemType::Execution
            }
            Self::ContextSummary | Self::ContextDetail | Self::ContextContent => ItemType::Context,
        }
    }

    pub fn type_name(&self) -> &str {
        match self {
            Self::ArtifactSummary(x) => &x[0].type_name,
            Self::ArtifactDetail(x) => &x.artifact.type_name,
            Self::ArtifactContent(x) => &x.artifact.type_name,
            _ => todo!(),
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
pub enum HookOutput {
    ArtifactSummary(Vec<crate::mlmd::artifact::Artifact>),
    ArtifactDetail(ArtifactDetailHookOutput),
    ArtifactContent(GeneralOutput),
    ExecutionSummary,
    ExecutionDetail,
    ExecutionContent,
    ContextSummary,
    ContextDetail,
    ContextContent,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ArtifactDetailHookOutput {
    pub artifact: crate::mlmd::artifact::Artifact,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GeneralOutput {
    Json(String),
    Markdown(String),
    Html(String),
}
