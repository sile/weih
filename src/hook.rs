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

    pub async fn run_artifact_detail_hook(
        &self,
        artifact: crate::mlmd::artifact::ArtifactDetail,
    ) -> actix_web::error::Result<crate::mlmd::artifact::ArtifactDetail> {
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
    ArtifactSummary,
    ArtifactDetail(ArtifactDetailHookInput),
    ArtifactContent,
    ArtifactProperty,
    ExecutionSummary,
    ExecutionDetail,
    ExecutionProperty,
    ContextSummary,
    ContextDetail,
    ContextProperty,
}

impl HookInput {
    pub fn item_type(&self) -> ItemType {
        match self {
            Self::ArtifactSummary
            | Self::ArtifactDetail(_)
            | Self::ArtifactContent
            | Self::ArtifactProperty => ItemType::Artifact,
            Self::ExecutionSummary | Self::ExecutionDetail | Self::ExecutionProperty => {
                ItemType::Execution
            }
            Self::ContextSummary | Self::ContextDetail | Self::ContextProperty => ItemType::Context,
        }
    }

    pub fn type_name(&self) -> &str {
        match self {
            Self::ArtifactDetail(x) => &x.artifact.type_name,
            _ => todo!(),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ArtifactDetailHookInput {
    pub artifact: crate::mlmd::artifact::ArtifactDetail,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum HookOutput {
    ArtifactSummary,
    ArtifactDetail(ArtifactDetailHookOutput),
    ArtifactContent,
    ArtifactProperty,
    ExecutionSummary,
    ExecutionDetail,
    ExecutionProperty,
    ContextSummary,
    ContextDetail,
    ContextProperty,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ArtifactDetailHookOutput {
    pub artifact: crate::mlmd::artifact::ArtifactDetail,
}
