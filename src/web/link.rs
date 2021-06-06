#[derive(Debug, Clone)]
pub enum Link {
    ArtifactType(i32),
}

impl std::fmt::Display for Link {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::ArtifactType(x) => write!(f, "[{}](/artifact_types/{})", x, x),
        }
    }
}
