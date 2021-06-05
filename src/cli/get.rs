pub mod artifact_types;
pub mod artifacts;

#[derive(Debug, structopt::StructOpt)]
#[structopt(rename_all = "kebab-case")]
pub enum GetOpt {
    ArtifactTypes(self::artifact_types::GetArtifactTypesOpt),
    Artifacts(self::artifacts::GetArtifactsOpt),
}

impl GetOpt {
    pub async fn execute(&self) -> anyhow::Result<()> {
        match self {
            Self::ArtifactTypes(o) => o.execute().await,
            Self::Artifacts(o) => o.execute().await,
        }
    }
}
