use crate::cli;
use crate::mlmd::artifact::ArtifactTypeDetail;

#[derive(Debug, structopt::StructOpt)]
#[structopt(rename_all = "kebab-case")]
pub struct ShowArtifactTypeOpt {
    #[structopt(flatten)]
    pub store: cli::common::MetadataStoreOpt,
    pub type_name: String,
}

impl ShowArtifactTypeOpt {
    pub async fn execute(&self) -> anyhow::Result<()> {
        let mut store = self.store.connect().await?;
        let types = store
            .get_artifact_types()
            .name(&self.type_name)
            .execute()
            .await?;
        if let Some(ty) = types.get(0).cloned() {
            cli::io::print_json(&ArtifactTypeDetail::from(ty))?;
        } else {
            anyhow::bail!("no such artifact type: {:?}", self.type_name);
        }
        Ok(())
    }
}
