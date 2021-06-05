use crate::cli;
use crate::mlmd::artifact::{ArtifactOrderByField, ArtifactSummary};
use crate::mlmd::context::ContextIdOrName;
use std::collections::{HashMap, HashSet};

#[derive(Debug, structopt::StructOpt)]
#[structopt(rename_all = "kebab-case")]
pub struct GetArtifactsOpt {
    #[structopt(flatten)]
    pub store: cli::common::MetadataStoreOpt,

    #[structopt(long = "type")]
    pub type_name: Option<String>,

    #[structopt(long)]
    pub context: Option<ContextIdOrName>,

    #[structopt(long, default_value = "1000")]
    pub limit: usize,

    #[structopt(long)]
    pub offset: Option<usize>,

    #[structopt(long, possible_values=ArtifactOrderByField::POSSIBLE_VALUES)]
    pub order_by: Option<ArtifactOrderByField>,

    #[structopt(long)]
    pub desc: bool,
}

impl GetArtifactsOpt {
    pub async fn execute(&self) -> anyhow::Result<()> {
        let mut store = self.store.connect().await?;

        let artifacts = self.get_artifacts(&mut store).await?;
        let artifact_types = self.get_artifact_types(&mut store, &artifacts).await?;
        cli::io::print_json_lines::<ArtifactSummary, _>(
            artifacts
                .into_iter()
                .map(|x| (artifact_types[&x.type_id].clone(), x)),
        )?;
        Ok(())
    }

    async fn get_artifacts(
        &self,
        store: &mut mlmd::MetadataStore,
    ) -> anyhow::Result<Vec<mlmd::metadata::Artifact>> {
        let context_id = if let Some(context) = &self.context {
            Some(context.resolve_id(store).await?)
        } else {
            None
        };

        let mut request = store.get_artifacts().limit(self.limit);
        if let Some(c) = context_id {
            request = request.context(c)
        }
        if let Some(n) = self.offset {
            request = request.offset(n);
        }
        if let Some(n) = &self.type_name {
            request = request.ty(n);
        }
        if let Some(field) = self.order_by {
            request = request.order_by(field.into(), !self.desc);
        }

        Ok(request.execute().await?)
    }

    async fn get_artifact_types(
        &self,
        store: &mut mlmd::MetadataStore,
        artifacts: &[mlmd::metadata::Artifact],
    ) -> anyhow::Result<HashMap<mlmd::metadata::TypeId, mlmd::metadata::ArtifactType>> {
        let artifact_type_ids = artifacts.iter().map(|x| x.type_id).collect::<HashSet<_>>();
        Ok(store
            .get_artifact_types()
            .ids(artifact_type_ids.into_iter())
            .execute()
            .await?
            .into_iter()
            .map(|x| (x.id, x))
            .collect())
    }
}
