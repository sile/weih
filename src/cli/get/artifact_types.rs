use crate::cli;
use crate::mlmd::artifact::ArtifactTypeSummary;

#[derive(Debug, structopt::StructOpt)]
#[structopt(rename_all = "kebab-case")]
pub struct GetArtifactTypesOpt {
    #[structopt(flatten)]
    pub store: cli::common::MetadataStoreOpt,
}

impl GetArtifactTypesOpt {
    pub async fn execute(&self) -> anyhow::Result<()> {
        let mut store = self.store.connect().await?;
        let types = store.get_artifact_types().execute().await?;
        cli::io::print_json_lines::<ArtifactTypeSummary, _>(types.into_iter())?;
        Ok(())
    }
}
