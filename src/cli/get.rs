pub mod artifact_types;

#[derive(Debug, structopt::StructOpt)]
#[structopt(rename_all = "kebab-case")]
pub enum GetOpt {
    ArtifactTypes(self::artifact_types::GetArtifactTypesOpt),
}

impl GetOpt {
    pub async fn execute(&self) -> anyhow::Result<()> {
        match self {
            Self::ArtifactTypes(o) => o.execute().await,
        }
    }
}
