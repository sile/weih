use crate::cli;
use crate::web;
use std::path::PathBuf;

#[derive(Debug, structopt::StructOpt)]
pub struct RunOpt {
    #[structopt(flatten)]
    pub store: cli::common::MetadataStoreOpt,

    #[structopt(long, default_value = "127.0.0.1")]
    pub addr: std::net::IpAddr,

    #[structopt(long, default_value = "3030")]
    pub port: u16,

    #[structopt(long)]
    pub hook: Option<PathBuf>,
}

impl RunOpt {
    pub async fn execute(&self) -> anyhow::Result<()> {
        let bind_addr = std::net::SocketAddr::from((self.addr, self.port));

        let hook_runner = if let Some(hook_config_path) = &self.hook {
            let f = std::fs::File::open(hook_config_path)?;
            let mut opts = Vec::new();
            for opt in serde_json::Deserializer::from_reader(f).into_iter() {
                opts.push(opt?);
            }
            crate::hook::HookRunner::new(&opts)
        } else {
            crate::hook::HookRunner::new(&[])
        };

        web::http_server_run(bind_addr, self.store.database.clone(), hook_runner).await
    }
}
