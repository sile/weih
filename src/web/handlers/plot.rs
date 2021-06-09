use crate::hook::GeneralOutput;
use crate::mlmd::artifact::{Artifact, ArtifactOrderByField};
use crate::web::handlers::artifacts::GetArtifactsQuery;
use crate::web::{response, Config};
use actix_web::{get, web, HttpResponse};
use plotly::{Histogram, Plot};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::io::Read;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct PlotHistogramQuery {
    // #[serde(flatten)]
    // artifacts: GetArtifactsQuery,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub type_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<usize>,
    #[serde(default)]
    pub order_by: ArtifactOrderByField,
    #[serde(default)]
    pub asc: bool,

    #[serde(default)]
    pub metric: Option<String>,

    #[serde(default)]
    pub group_key: Option<String>,

    #[serde(default)]
    pub group_value: Option<String>,

    #[serde(default)]
    pub do_plot: bool,
}

impl PlotHistogramQuery {
    fn artifacts(&self) -> GetArtifactsQuery {
        GetArtifactsQuery {
            type_name: self.type_name.clone(),
            name: self.name.clone(),
            context: self.context.clone(),
            limit: self.limit.clone(),
            offset: self.offset.clone(),
            order_by: self.order_by.clone(),
            asc: self.asc.clone(),
        }
    }

    fn metric(&self, name: &str) -> Self {
        let mut this = self.clone();
        this.metric = Some(name.to_owned());
        this
    }

    fn group(&self, key: &str, value: &str) -> Self {
        let mut this = self.clone();
        this.group_key = Some(key.to_owned());
        this.group_value = Some(value.to_owned());
        this
    }

    fn do_plot(&self) -> Self {
        let mut this = self.clone();
        this.do_plot = true;
        this
    }

    fn to_url(&self) -> String {
        let mut qs = self.artifacts().to_qs();
        if let Some(x) = &self.metric {
            qs += &format!("&metric={}", x);
        }
        if let Some(x) = &self.group_key {
            qs += &format!("&group-key={}", x);
        }
        if let Some(x) = &self.group_value {
            qs += &format!("&group-value={}", x);
        }
        if self.do_plot {
            qs += &format!("&do-plot=true");
        }
        format!("/plot/histogram?{}", qs)
    }
}

#[get("/plot/histogram")]
pub async fn plot_histogram(
    config: web::Data<Config>,
    query: web::Query<PlotHistogramQuery>,
) -> actix_web::Result<HttpResponse> {
    let mut store = config.connect_metadata_store().await?;
    let artifacts = query
        .artifacts()
        .get_artifacts(&mut store)
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;

    let mut md = format!(
        "# Histogram Plot ([target artifacts]({}))\n",
        query.artifacts().to_url()
    );

    let metric_property = if let Some(metric) = &query.metric {
        md += &format!("- Metric property: {}\n", metric);
        metric
    } else {
        md += "Select metric value property:\n";
        let mut candidates = BTreeSet::new();
        for a in &artifacts {
            for (k, v) in &a.properties {
                if is_int_or_double(v) {
                    candidates.insert(k);
                }
            }
        }
        for c in candidates {
            md += &format!("- [{}]({})\n", c, query.metric(c).to_url());
        }
        return Ok(response::markdown(&md));
    };

    if query.group_key.is_none() {
        md += "\nSelect target property (optional):\n";
        let mut candidates = BTreeSet::new();
        for a in &artifacts {
            for (k, v) in &a.properties {
                if let (k, Some(v)) = (k, get_string(v)) {
                    candidates.insert((k, v));
                }
            }
        }
        for (k, v) in candidates {
            md += &format!("- [{}] [{}]({})\n", k, v, query.group(k, v).to_url());
        }
    } else {
        md += &format!(
            "- Target property: {}={}\n",
            query.group_key.as_ref().unwrap(),
            query.group_value.as_ref().unwrap()
        );
    }

    let mut metrics = Vec::new();
    for a in &artifacts {
        if let (Some(tk), Some(tv)) = (query.group_key.as_ref(), query.group_value.as_ref()) {
            let mut is_target = false;
            for (k, v) in &a.properties {
                if k == tk && get_string(v) == Some(tv) {
                    is_target = true;
                }
            }
            if !is_target {
                continue;
            }
        }

        for (k, v) in &a.properties {
            if k != metric_property {
                continue;
            }
            if let Some(m) = get_double(v) {
                metrics.push(m);
                break;
            }
        }
    }

    if !query.do_plot {
        md += &format!("- n_target: {}\n", metrics.len());
        md += &format!("\n**[Do plot]({})**\n", query.do_plot().to_url());
        return Ok(response::markdown(&md));
    }

    let mut plot = Plot::new();
    let trace = Histogram::new(metrics);
    plot.add_trace(trace);

    let mut html = String::new();
    let mut html_file = tempfile::NamedTempFile::new()?;
    plot.to_html(html_file.path());
    html_file.read_to_string(&mut html)?;
    Ok(response::html(&html))
}

fn is_int_or_double(p: &mlmd::metadata::PropertyValue) -> bool {
    matches!(p, mlmd::metadata::PropertyValue::Int(_))
        || matches!(p, mlmd::metadata::PropertyValue::Double(_))
}

fn get_string(p: &mlmd::metadata::PropertyValue) -> Option<&str> {
    if let mlmd::metadata::PropertyValue::String(v) = p {
        Some(v)
    } else {
        None
    }
}

fn get_double(p: &mlmd::metadata::PropertyValue) -> Option<f64> {
    match p {
        mlmd::metadata::PropertyValue::Int(v) => Some(*v as f64),
        mlmd::metadata::PropertyValue::Double(v) => Some(*v),
        _ => None,
    }
}

#[get("/plot/scatter")]
pub async fn plot_scatter(
    config: web::Data<Config>,
    query: web::Query<GetArtifactsQuery>,
) -> actix_web::Result<HttpResponse> {
    let mut store = config.connect_metadata_store().await?;
    let artifacts = query
        .get_artifacts(&mut store)
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;

    let mut md = format!("# Scatter Plot ([target artifacts]({}))\n", query.to_url());

    Ok(response::markdown("TODO"))
}
