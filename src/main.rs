use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(rename_all = "kebab-case")]
enum Opt {
    Get(weih::cli::get::GetOpt),
    Run,
    Show,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();
    match opt {
        Opt::Get(o) => o.execute().await?,
        Opt::Run => todo!(),
        Opt::Show => todo!(),
    }
    Ok(())
}
