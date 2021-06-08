use crate::mlmd::event::Event;
use crate::web::{response, Config};
use actix_web::{get, web, HttpResponse};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct GetEventsQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<usize>,
    #[serde(default)]
    pub asc: bool,
}

impl GetEventsQuery {
    pub fn limit(&self) -> usize {
        self.limit.unwrap_or(100)
    }

    pub fn offset(&self) -> usize {
        self.offset.unwrap_or(0)
    }

    fn prev(&self) -> Self {
        let mut this = self.clone();
        this.offset = Some(self.offset().saturating_sub(self.limit()));
        this
    }

    fn next(&self) -> Self {
        let mut this = self.clone();
        this.offset = Some(self.offset() + self.limit());
        this
    }

    pub fn order_by_ctime(&self, asc: bool) -> Self {
        let mut this = self.clone();
        this.asc = asc;
        this.offset = None;
        this
    }

    pub fn execution(&self, id: i32) -> Self {
        let mut this = self.clone();
        this.execution = Some(id);
        this.offset = None;
        this
    }

    pub fn artifact(&self, id: i32) -> Self {
        let mut this = self.clone();
        this.artifact = Some(id);
        this.offset = None;
        this
    }

    pub fn to_url(&self) -> String {
        let mut s = format!(
            "/events/?limit={}&offset={}&asc={}",
            self.limit(),
            self.offset(),
            self.asc
        );
        if let Some(x) = self.artifact {
            s += &format!("&artifact={}", x);
        }
        if let Some(x) = self.execution {
            s += &format!("&execution={}", x);
        }
        s
    }
}

#[get("/events/")]
async fn get_events(
    config: web::Data<Config>,
    query: web::Query<GetEventsQuery>,
) -> actix_web::Result<HttpResponse> {
    let mut store = config.connect_metadata_store().await?;
    let mut request = store
        .get_events()
        .limit(query.limit())
        .offset(query.offset())
        .order_by(mlmd::requests::EventOrderByField::CreateTime, query.asc);

    if let Some(x) = query.artifact {
        request = request.artifact(mlmd::metadata::ArtifactId::new(x));
    }
    if let Some(x) = query.execution {
        request = request.execution(mlmd::metadata::ExecutionId::new(x));
    }

    let events = request
        .execute()
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;

    let mut md = "# Events\n".to_string();

    if query.offset() != 0 {
        md += &format!(" [<<]({})", query.prev().to_url());
    } else {
        md += " <<";
    }
    md += &format!(" {}~{} ", query.offset() + 1, query.offset() + events.len());
    if events.len() == query.limit() {
        md += &format!("[>>]({})", query.next().to_url());
    } else {
        md += ">>";
    }

    md += "\n";
    md += &format!(
        "| execution | artifact | type | path | time{} |\n",
        if query.asc {
            format!("<[>]({})", query.order_by_ctime(!query.asc).to_url())
        } else {
            format!("[<]({})>", query.order_by_ctime(!query.asc).to_url())
        }
    );
    md += "|-----------|----------|------|------|------|\n";

    for event in events {
        let event = Event::from(event);
        md += &format!(
            "| [{}]({}) [@](/executions/{}) | [{}]({}) [@](/artifacts/{}) | {} | {} | {} | \n",
            event.execution_id,
            query.execution(event.execution_id).to_url(),
            event.execution_id,
            event.artifact_id,
            query.artifact(event.artifact_id).to_url(),
            event.artifact_id,
            event.ty,
            event
                .path
                .iter()
                .map(|x| x.to_string())
                .collect::<Vec<_>>()
                .join(","),
            event.time,
        );
    }

    Ok(response::markdown(&md))
}
