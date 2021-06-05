use crate::mlmd::property::PropertyType;
use crate::time::DateTime;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ArtifactTypeSummary {
    pub id: i32,
    pub name: String,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub properties: BTreeSet<String>,
}

impl From<mlmd::metadata::ArtifactType> for ArtifactTypeSummary {
    fn from(x: mlmd::metadata::ArtifactType) -> Self {
        Self {
            id: x.id.get(),
            name: x.name,
            properties: x.properties.into_iter().map(|(k, _)| k).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ArtifactTypeDetail {
    pub id: i32,
    pub name: String,
    pub properties: BTreeMap<String, PropertyType>,
}

impl From<mlmd::metadata::ArtifactType> for ArtifactTypeDetail {
    fn from(x: mlmd::metadata::ArtifactType) -> Self {
        Self {
            id: x.id.get(),
            name: x.name,
            properties: x
                .properties
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ArtifactSummary {
    pub id: i32,
    #[serde(rename = "type")]
    pub type_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub state: ArtifactState,
    pub ctime: DateTime,
    pub utime: DateTime,
}

impl From<(mlmd::metadata::ArtifactType, mlmd::metadata::Artifact)> for ArtifactSummary {
    fn from(x: (mlmd::metadata::ArtifactType, mlmd::metadata::Artifact)) -> Self {
        Self {
            id: x.1.id.get(),
            type_name: x.0.name,
            name: x.1.name,
            state: x.1.state.into(),
            ctime: crate::time::duration_to_datetime(x.1.create_time_since_epoch),
            utime: crate::time::duration_to_datetime(x.1.last_update_time_since_epoch),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ArtifactState {
    Unknown,
    Pending,
    Live,
    MarkedForDeletion,
    Deleted,
}

impl From<mlmd::metadata::ArtifactState> for ArtifactState {
    fn from(x: mlmd::metadata::ArtifactState) -> Self {
        use mlmd::metadata::ArtifactState::*;

        match x {
            Unknown => Self::Unknown,
            Pending => Self::Pending,
            Live => Self::Live,
            MarkedForDeletion => Self::MarkedForDeletion,
            Deleted => Self::Deleted,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactOrderByField {
    Name,
    CreateTime,
    UpdateTime,
}

impl ArtifactOrderByField {
    pub const POSSIBLE_VALUES: &'static [&'static str] = &["name", "ctime", "utime"];
}

impl std::str::FromStr for ArtifactOrderByField {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s {
            "name" => Ok(Self::Name),
            "ctime" => Ok(Self::CreateTime),
            "utime" => Ok(Self::UpdateTime),
            _ => anyhow::bail!("invalid value: {:?}", s),
        }
    }
}

impl From<ArtifactOrderByField> for mlmd::requests::ArtifactOrderByField {
    fn from(x: ArtifactOrderByField) -> Self {
        match x {
            ArtifactOrderByField::Name => Self::Name,
            ArtifactOrderByField::CreateTime => Self::CreateTime,
            ArtifactOrderByField::UpdateTime => Self::UpdateTime,
        }
    }
}
