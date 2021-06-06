use crate::cli;
use crate::hook::ItemType;
use std::path::PathBuf;

#[derive(Debug, structopt::StructOpt, serde::Serialize, serde::Deserialize)]
#[structopt(rename_all = "kebab-case")]
#[serde(rename_all = "kebab-case")]
pub struct HookOpt {
    #[serde(rename = "item")]
    #[structopt(long = "item", possible_values=ItemType::POSSIBLE_VALUES)]
    pub item_type: ItemType,

    #[serde(rename = "type")]
    #[structopt(long = "type")]
    pub type_name: String,

    pub command: PathBuf,

    pub args: Vec<String>,
}

impl HookOpt {
    pub async fn execute(&self) -> anyhow::Result<()> {
        cli::io::print_json(self)?;
        Ok(())
    }
}
