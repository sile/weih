use crate::cli;
use crate::mlmd::artifact::{ArtifactDetail, ArtifactIdOrName};

#[derive(Debug, structopt::StructOpt)]
#[structopt(rename_all = "kebab-case")]
pub struct ShowArtifactOpt {
    #[structopt(flatten)]
    pub store: cli::common::MetadataStoreOpt,
    pub artifact: ArtifactIdOrName,
}

impl ShowArtifactOpt {
    pub async fn execute(&self) -> anyhow::Result<()> {
        let mut store = self.store.connect().await?;
        let id = self.artifact.resolve_id(&mut store).await?;
        let artifacts = store.get_artifacts().id(id).execute().await?;
        anyhow::ensure!(
            !artifacts.is_empty(),
            "no such artifact: {:?}",
            self.artifact
        );

        let types = store
            .get_artifact_types()
            .id(artifacts[0].type_id)
            .execute()
            .await?;
        anyhow::ensure!(!types.is_empty(), "unreachable");

        let detail = ArtifactDetail::from((types[0].clone(), artifacts[0].clone()));
        cli::io::print_json(&detail)?;
        Ok(())
    }
}
