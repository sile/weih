use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(rename_all = "kebab-case")]
enum Opt {
    Get(weih::cli::get::GetOpt),
    Run(weih::cli::run::RunOpt),
    Show(weih::cli::show::ShowOpt),
}

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();
    match opt {
        Opt::Get(o) => o.execute().await?,
        Opt::Run(o) => o.execute().await?,
        Opt::Show(o) => o.execute().await?,
    }
    Ok(())
}
