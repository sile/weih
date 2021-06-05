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
