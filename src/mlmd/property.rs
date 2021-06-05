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
