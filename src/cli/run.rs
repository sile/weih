use crate::cli;
use crate::web;

#[derive(Debug, structopt::StructOpt)]
pub struct RunOpt {
    #[structopt(flatten)]
    pub store: cli::common::MetadataStoreOpt,

    #[structopt(long, default_value = "127.0.0.1")]
    pub addr: std::net::IpAddr,

    #[structopt(long, default_value = "3030")]
    pub port: u16,
}

impl RunOpt {
    pub async fn execute(&self) -> anyhow::Result<()> {
        let bind_addr = std::net::SocketAddr::from((self.addr, self.port));
        web::http_server_run(bind_addr, self.store.database.clone()).await
    }
}
