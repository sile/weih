use crate::time::DateTime;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Event {
    pub artifact_id: i32,
    pub execution_id: i32,
    pub ty: EventType,
    pub path: Vec<EventStep>,
    pub time: DateTime,
}

impl From<mlmd::metadata::Event> for Event {
    fn from(x: mlmd::metadata::Event) -> Self {
        Self {
            artifact_id: x.artifact_id.get(),
            execution_id: x.execution_id.get(),
            ty: x.ty.into(),
            path: x.path.into_iter().map(From::from).collect(),
            time: crate::time::duration_to_datetime(x.create_time_since_epoch),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EventType {
    Unknown,
    DeclaredOutput,
    DeclaredInput,
    Input,
    Output,
    InternalInput,
    InternalOutput,
}

impl std::fmt::Display for EventType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Unknown => write!(f, "UNKNOWN"),
            Self::DeclaredOutput => write!(f, "DECLARED_OUTPUT"),
            Self::DeclaredInput => write!(f, "DECLARED_INPUT"),
            Self::Input => write!(f, "INPUT"),
            Self::Output => write!(f, "OUTPUT"),
            Self::InternalInput => write!(f, "INTERNAL_INPUT"),
            Self::InternalOutput => write!(f, "INTERNAL_OUTPUT"),
        }
    }
}

impl From<mlmd::metadata::EventType> for EventType {
    fn from(x: mlmd::metadata::EventType) -> Self {
        use mlmd::metadata::EventType::*;

        match x {
            Unknown => Self::Unknown,
            DeclaredOutput => Self::DeclaredOutput,
            DeclaredInput => Self::DeclaredInput,
            Input => Self::Input,
            Output => Self::Output,
            InternalInput => Self::InternalInput,
            InternalOutput => Self::InternalOutput,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum EventStep {
    Index(i32),
    Key(String),
}

impl std::fmt::Display for EventStep {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Index(x) => write!(f, "{}", x),
            Self::Key(x) => write!(f, "{}", x),
        }
    }
}

impl From<mlmd::metadata::EventStep> for EventStep {
    fn from(x: mlmd::metadata::EventStep) -> Self {
        use mlmd::metadata::EventStep::*;

        match x {
            Index(x) => Self::Index(x),
            Key(x) => Self::Key(x),
        }
    }
}
