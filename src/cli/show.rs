pub mod artifact;
pub mod artifact_type;

#[derive(Debug, structopt::StructOpt)]
#[structopt(rename_all = "kebab-case")]
pub enum ShowOpt {
    ArtifactType(self::artifact_type::ShowArtifactTypeOpt),
    Artifact(self::artifact::ShowArtifactOpt),
}

impl ShowOpt {
    pub async fn execute(&self) -> anyhow::Result<()> {
        match self {
            Self::ArtifactType(o) => o.execute().await,
            Self::Artifact(o) => o.execute().await,
        }
    }
}
