use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(rename_all = "kebab-case")]
enum Opt {
    Hook(weih::cli::hook::HookOpt),
    Run(weih::cli::run::RunOpt),
}

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();
    match opt {
        Opt::Hook(o) => o.execute().await?,
        Opt::Run(o) => o.execute().await?,
    }
    Ok(())
}
