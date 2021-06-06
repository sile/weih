#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum PropertyType {
    Int,
    Double,
    String,
}

impl From<mlmd::metadata::PropertyType> for PropertyType {
    fn from(x: mlmd::metadata::PropertyType) -> Self {
        use mlmd::metadata::PropertyType::*;

        match x {
            Int => Self::Int,
            Double => Self::Double,
            String => Self::String,
        }
    }
}

impl std::fmt::Display for PropertyType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Int => write!(f, "INT"),
            Self::Double => write!(f, "DOUBLE"),
            Self::String => write!(f, "STRING"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum PropertyValue {
    Int(i32),
    Double(f64),
    String(String),
}

impl From<mlmd::metadata::PropertyValue> for PropertyValue {
    fn from(x: mlmd::metadata::PropertyValue) -> Self {
        use mlmd::metadata::PropertyValue::*;

        match x {
            Int(x) => Self::Int(x),
            Double(x) => Self::Double(x),
            String(x) => Self::String(x),
        }
    }
}
