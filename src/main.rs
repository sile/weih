use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(rename_all = "kebab-case")]
enum Opt {
    Get(weih::cli::get::GetOpt),
    Run(weih::cli::run::RunOpt),
    Show(weih::cli::show::ShowOpt),
}

fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();
    match opt {
        Opt::Get(_) | Opt::Show(_) => tokio_main(opt)?,
        Opt::Run(_) => actix_web_main(opt)?,
    }
    Ok(())
}

#[tokio::main]
async fn tokio_main(opt: Opt) -> anyhow::Result<()> {
    match opt {
        Opt::Get(o) => o.execute().await?,
        Opt::Show(o) => o.execute().await?,
        _ => unreachable!(),
    }
    Ok(())
}

#[actix_web::main]
async fn actix_web_main(opt: Opt) -> anyhow::Result<()> {
    match opt {
        Opt::Run(o) => o.execute().await?,
        _ => unreachable!(),
    }
    Ok(())
}
