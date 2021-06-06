#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ItemType {
    Artifact,
    Execution,
    Context,
}

impl std::str::FromStr for ItemType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s {
            "artifact" => Ok(Self::Artifact),
            "execution" => Ok(Self::Execution),
            "context" => Ok(Self::Context),
            _ => anyhow::bail!("unknown item type: {:?}", s),
        }
    }
}

impl ItemType {
    pub const POSSIBLE_VALUES: &'static [&'static str] = &["artifact", "execution", "context"];
}
