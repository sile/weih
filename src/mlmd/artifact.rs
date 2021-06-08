use crate::mlmd::property::{PropertyType, PropertyValue};
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

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Artifact {
    pub id: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(rename = "type")]
    pub type_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
    pub state: ArtifactState,
    pub ctime: DateTime,
    pub mtime: DateTime,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub properties: BTreeMap<String, PropertyValue>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub custom_properties: BTreeMap<String, PropertyValue>,

    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra_properties: BTreeMap<String, PropertyValue>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
}

impl From<(mlmd::metadata::ArtifactType, mlmd::metadata::Artifact)> for Artifact {
    fn from(x: (mlmd::metadata::ArtifactType, mlmd::metadata::Artifact)) -> Self {
        Self {
            id: x.1.id.get(),
            type_name: x.0.name,
            name: x.1.name,
            state: x.1.state.into(),
            uri: x.1.uri,
            ctime: crate::time::duration_to_datetime(x.1.create_time_since_epoch),
            mtime: crate::time::duration_to_datetime(x.1.last_update_time_since_epoch),
            properties: x
                .1
                .properties
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
            custom_properties: x
                .1
                .custom_properties
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
            extra_properties: BTreeMap::new(),
            summary: None,
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

impl std::fmt::Display for ArtifactState {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Unknown => write!(f, "UNKNOWN"),
            Self::Pending => write!(f, "PENDING"),
            Self::Live => write!(f, "Live"),
            Self::MarkedForDeletion => write!(f, "MARKED_FOR_DELETION"),
            Self::Deleted => write!(f, "DELETED"),
        }
    }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ArtifactOrderByField {
    Id,
    Name,
    CreateTime,
    UpdateTime,
}

impl ArtifactOrderByField {
    pub const POSSIBLE_VALUES: &'static [&'static str] = &["id", "name", "ctime", "mtime"];
}

impl Default for ArtifactOrderByField {
    fn default() -> Self {
        Self::Id
    }
}

impl std::str::FromStr for ArtifactOrderByField {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s {
            "id" => Ok(Self::Id),
            "name" => Ok(Self::Name),
            "ctime" => Ok(Self::CreateTime),
            "mtime" => Ok(Self::UpdateTime),
            _ => anyhow::bail!("invalid value: {:?}", s),
        }
    }
}

impl From<ArtifactOrderByField> for mlmd::requests::ArtifactOrderByField {
    fn from(x: ArtifactOrderByField) -> Self {
        match x {
            ArtifactOrderByField::Id => Self::Id,
            ArtifactOrderByField::Name => Self::Name,
            ArtifactOrderByField::CreateTime => Self::CreateTime,
            ArtifactOrderByField::UpdateTime => Self::UpdateTime,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ArtifactIdOrName {
    Id(mlmd::metadata::ArtifactId),
    Name {
        artifact_name: String,
        type_name: String,
    },
}

impl ArtifactIdOrName {
    pub async fn resolve_id(
        &self,
        store: &mut mlmd::MetadataStore,
    ) -> anyhow::Result<mlmd::metadata::ArtifactId> {
        match self {
            Self::Id(id) => Ok(*id),
            Self::Name {
                type_name,
                artifact_name,
            } => {
                let artifacts = store
                    .get_artifacts()
                    .type_and_name(type_name, artifact_name)
                    .execute()
                    .await?;
                if let Some(id) = artifacts.get(0).map(|c| c.id) {
                    Ok(id)
                } else {
                    anyhow::bail!(
                        "no such artifact: type={:?}, name={:?}",
                        type_name,
                        artifact_name
                    );
                }
            }
        }
    }
}

impl std::str::FromStr for ArtifactIdOrName {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        let mut tokens = s.splitn(2, '@');
        let id_or_artifact_name = tokens.next().expect("unreachable");
        if let Some(type_name) = tokens.next() {
            Ok(Self::Name {
                artifact_name: id_or_artifact_name.to_string(),
                type_name: type_name.to_string(),
            })
        } else {
            let id = mlmd::metadata::ArtifactId::new(id_or_artifact_name.parse()?);
            Ok(Self::Id(id))
        }
    }
}
