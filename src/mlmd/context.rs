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
        let mut tokens = s.splitn(1, '@');
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
