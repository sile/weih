use crate::mlmd::property::{PropertyType, PropertyValue};
use crate::time::DateTime;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ContextTypeSummary {
    pub id: i32,
    pub name: String,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub properties: BTreeSet<String>,
}

impl From<mlmd::metadata::ContextType> for ContextTypeSummary {
    fn from(x: mlmd::metadata::ContextType) -> Self {
        Self {
            id: x.id.get(),
            name: x.name,
            properties: x.properties.into_iter().map(|(k, _)| k).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ContextTypeDetail {
    pub id: i32,
    pub name: String,
    pub properties: BTreeMap<String, PropertyType>,
}

impl From<mlmd::metadata::ContextType> for ContextTypeDetail {
    fn from(x: mlmd::metadata::ContextType) -> Self {
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ContextIdOrName {
    Id(mlmd::metadata::ContextId),
    Name {
        context_name: String,
        type_name: String,
    },
}

impl ContextIdOrName {
    pub async fn resolve_id(
        &self,
        store: &mut mlmd::MetadataStore,
    ) -> anyhow::Result<mlmd::metadata::ContextId> {
        match self {
            Self::Id(id) => Ok(*id),
            Self::Name {
                type_name,
                context_name,
            } => {
                let contexts = store
                    .get_contexts()
                    .type_and_name(type_name, context_name)
                    .execute()
                    .await?;
                if let Some(id) = contexts.get(0).map(|c| c.id) {
                    Ok(id)
                } else {
                    anyhow::bail!(
                        "no such context: type={:?}, name={:?}",
                        type_name,
                        context_name
                    );
                }
            }
        }
    }
}

impl std::str::FromStr for ContextIdOrName {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        let mut tokens = s.splitn(2, '@');
        let id_or_context_name = tokens.next().expect("unreachable");
        if let Some(type_name) = tokens.next() {
            Ok(Self::Name {
                context_name: id_or_context_name.to_string(),
                type_name: type_name.to_string(),
            })
        } else {
            let id = mlmd::metadata::ContextId::new(id_or_context_name.parse()?);
            Ok(Self::Id(id))
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Context {
    pub id: i32,
    pub name: String,
    #[serde(rename = "type")]
    pub type_name: String,
    pub ctime: DateTime,
    pub mtime: DateTime,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub properties: BTreeMap<String, PropertyValue>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub custom_properties: BTreeMap<String, PropertyValue>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
}

impl From<(mlmd::metadata::ContextType, mlmd::metadata::Context)> for Context {
    fn from(x: (mlmd::metadata::ContextType, mlmd::metadata::Context)) -> Self {
        Self {
            id: x.1.id.get(),
            type_name: x.0.name,
            name: x.1.name,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ContextOrderByField {
    Id,
    Name,
    CreateTime,
    UpdateTime,
}

impl ContextOrderByField {
    pub const POSSIBLE_VALUES: &'static [&'static str] = &["id", "name", "ctime", "mtime"];
}

impl Default for ContextOrderByField {
    fn default() -> Self {
        Self::Id
    }
}

impl std::str::FromStr for ContextOrderByField {
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

impl From<ContextOrderByField> for mlmd::requests::ContextOrderByField {
    fn from(x: ContextOrderByField) -> Self {
        match x {
            ContextOrderByField::Id => Self::Id,
            ContextOrderByField::Name => Self::Name,
            ContextOrderByField::CreateTime => Self::CreateTime,
            ContextOrderByField::UpdateTime => Self::UpdateTime,
        }
    }
}
