use anyhow::Context as _;

#[derive(Debug, structopt::StructOpt)]
pub struct MetadataStoreOpt {
    #[structopt(long, name = "URI", env = "WEIH_MLMD_DB")]
    pub database: String,
}

impl MetadataStoreOpt {
    pub async fn connect(&self) -> anyhow::Result<mlmd::MetadataStore> {
        let store = mlmd::MetadataStore::connect(&self.database)
            .await
            .with_context(|| format!("cannot connect to the database: {:?}", self.database))?;
        Ok(store)
    }
}
