pub mod artifact_type;
// pub mod artifacts;

#[derive(Debug, structopt::StructOpt)]
#[structopt(rename_all = "kebab-case")]
pub enum ShowOpt {
    ArtifactType(self::artifact_type::ShowArtifactTypeOpt),
    // Artifacts(self::artifacts::ShowArtifactsOpt),
}

impl ShowOpt {
    pub async fn execute(&self) -> anyhow::Result<()> {
        match self {
            Self::ArtifactType(o) => o.execute().await,
            // Self::Artifacts(o) => o.execute().await,
        }
    }
}
