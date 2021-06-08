use crate::mlmd::property::{PropertyType, PropertyValue};
use crate::time::DateTime;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ExecutionTypeSummary {
    pub id: i32,
    pub name: String,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub properties: BTreeSet<String>,
}

impl From<mlmd::metadata::ExecutionType> for ExecutionTypeSummary {
    fn from(x: mlmd::metadata::ExecutionType) -> Self {
        Self {
            id: x.id.get(),
            name: x.name,
            properties: x.properties.into_iter().map(|(k, _)| k).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ExecutionTypeDetail {
    pub id: i32,
    pub name: String,
    pub properties: BTreeMap<String, PropertyType>,
}

impl From<mlmd::metadata::ExecutionType> for ExecutionTypeDetail {
    fn from(x: mlmd::metadata::ExecutionType) -> Self {
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
pub struct Execution {
    pub id: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(rename = "type")]
    pub type_name: String,
    pub state: ExecutionState,
    pub ctime: DateTime,
    pub mtime: DateTime,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub properties: BTreeMap<String, PropertyValue>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub custom_properties: BTreeMap<String, PropertyValue>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
}

impl From<(mlmd::metadata::ExecutionType, mlmd::metadata::Execution)> for Execution {
    fn from(x: (mlmd::metadata::ExecutionType, mlmd::metadata::Execution)) -> Self {
        Self {
            id: x.1.id.get(),
            type_name: x.0.name,
            name: x.1.name,
            state: x.1.last_known_state.into(),
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
            summary: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExecutionState {
    Unknown,
    New,
    Running,
    Complete,
    Failed,
    Cached,
    Canceled,
}

impl std::fmt::Display for ExecutionState {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Unknown => write!(f, "UNKNOWN"),
            Self::New => write!(f, "NEW"),
            Self::Running => write!(f, "RUNNING"),
            Self::Complete => write!(f, "COMPLETE"),
            Self::Failed => write!(f, "FAILED"),
            Self::Cached => write!(f, "CACHED"),
            Self::Canceled => write!(f, "CANCELED"),
        }
    }
}

impl From<mlmd::metadata::ExecutionState> for ExecutionState {
    fn from(x: mlmd::metadata::ExecutionState) -> Self {
        use mlmd::metadata::ExecutionState::*;

        match x {
            Unknown => Self::Unknown,
            New => Self::New,
            Running => Self::Running,
            Complete => Self::Complete,
            Failed => Self::Failed,
            Cached => Self::Cached,
            Canceled => Self::Canceled,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ExecutionOrderByField {
    Id,
    Name,
    CreateTime,
    UpdateTime,
}

impl ExecutionOrderByField {
    pub const POSSIBLE_VALUES: &'static [&'static str] = &["id", "name", "ctime", "mtime"];
}

impl Default for ExecutionOrderByField {
    fn default() -> Self {
        Self::Id
    }
}

impl std::str::FromStr for ExecutionOrderByField {
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

impl From<ExecutionOrderByField> for mlmd::requests::ExecutionOrderByField {
    fn from(x: ExecutionOrderByField) -> Self {
        match x {
            ExecutionOrderByField::Id => Self::Id,
            ExecutionOrderByField::Name => Self::Name,
            ExecutionOrderByField::CreateTime => Self::CreateTime,
            ExecutionOrderByField::UpdateTime => Self::UpdateTime,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ExecutionIdOrName {
    Id(mlmd::metadata::ExecutionId),
    Name {
        execution_name: String,
        type_name: String,
    },
}

impl ExecutionIdOrName {
    pub async fn resolve_id(
        &self,
        store: &mut mlmd::MetadataStore,
    ) -> anyhow::Result<mlmd::metadata::ExecutionId> {
        match self {
            Self::Id(id) => Ok(*id),
            Self::Name {
                type_name,
                execution_name,
            } => {
                let executions = store
                    .get_executions()
                    .type_and_name(type_name, execution_name)
                    .execute()
                    .await?;
                if let Some(id) = executions.get(0).map(|c| c.id) {
                    Ok(id)
                } else {
                    anyhow::bail!(
                        "no such execution: type={:?}, name={:?}",
                        type_name,
                        execution_name
                    );
                }
            }
        }
    }
}

impl std::str::FromStr for ExecutionIdOrName {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        let mut tokens = s.splitn(2, '@');
        let id_or_execution_name = tokens.next().expect("unreachable");
        if let Some(type_name) = tokens.next() {
            Ok(Self::Name {
                execution_name: id_or_execution_name.to_string(),
                type_name: type_name.to_string(),
            })
        } else {
            let id = mlmd::metadata::ExecutionId::new(id_or_execution_name.parse()?);
            Ok(Self::Id(id))
        }
    }
}
