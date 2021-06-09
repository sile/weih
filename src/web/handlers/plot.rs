use crate::mlmd::artifact::ArtifactOrderByField;
use crate::web::handlers::artifacts::GetArtifactsQuery;
use crate::web::{response, Config};
use actix_web::{get, web, HttpResponse};
use plotly::common::Mode;
use plotly::{Histogram, Plot, Scatter};
use std::collections::{BTreeMap, BTreeSet};
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

fn is_int(p: &mlmd::metadata::PropertyValue) -> bool {
    matches!(p, mlmd::metadata::PropertyValue::Int(_))
}

fn get_int(p: &mlmd::metadata::PropertyValue) -> Option<i32> {
    if let mlmd::metadata::PropertyValue::Int(n) = p {
        Some(*n)
    } else {
        None
    }
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct PlotScatterQuery {
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
    pub x_filter_key: Option<String>,

    #[serde(default)]
    pub x_filter_value: Option<String>,

    #[serde(default)]
    pub x_metric: Option<String>,

    #[serde(default)]
    pub y_filter_key: Option<String>,

    #[serde(default)]
    pub y_filter_value: Option<String>,

    #[serde(default)]
    pub y_metric: Option<String>,

    #[serde(default)]
    pub join_key: Option<String>,

    #[serde(default)]
    pub do_plot: bool,
}

impl PlotScatterQuery {
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

    fn x_metric(&self, name: &str) -> Self {
        let mut this = self.clone();
        this.x_metric = Some(name.to_owned());
        this
    }

    fn y_metric(&self, name: &str) -> Self {
        let mut this = self.clone();
        this.y_metric = Some(name.to_owned());
        this
    }

    fn join_key(&self, name: &str) -> Self {
        let mut this = self.clone();
        this.join_key = Some(name.to_owned());
        this
    }

    fn x_filter(&self, k: &str, v: &str) -> Self {
        let mut this = self.clone();
        this.x_filter_key = Some(k.to_owned());
        this.x_filter_value = Some(v.to_owned());
        this
    }

    fn y_filter(&self, k: &str, v: &str) -> Self {
        let mut this = self.clone();
        this.y_filter_key = Some(k.to_owned());
        this.y_filter_value = Some(v.to_owned());
        this
    }

    fn do_plot(&self) -> Self {
        let mut this = self.clone();
        this.do_plot = true;
        this
    }

    fn to_url(&self) -> String {
        let mut qs = self.artifacts().to_qs();
        if let Some(x) = &self.x_metric {
            qs += &format!("&x-metric={}", x);
        }
        if let Some(x) = &self.y_metric {
            qs += &format!("&y-metric={}", x);
        }
        if let Some(x) = &self.join_key {
            qs += &format!("&join-key={}", x);
        }
        if let Some(x) = &self.x_filter_key {
            qs += &format!("&x-filter-key={}", x);
        }
        if let Some(x) = &self.y_filter_key {
            qs += &format!("&y-filter-key={}", x);
        }
        if let Some(x) = &self.x_filter_value {
            qs += &format!("&x-filter-value={}", x);
        }
        if let Some(x) = &self.y_filter_value {
            qs += &format!("&y-filter-value={}", x);
        }
        if self.do_plot {
            qs += &format!("&do-plot=true");
        }
        format!("/plot/scatter?{}", qs)
    }
}

#[get("/plot/scatter")]
pub async fn plot_scatter(
    config: web::Data<Config>,
    query: web::Query<PlotScatterQuery>,
) -> actix_web::Result<HttpResponse> {
    let mut store = config.connect_metadata_store().await?;
    let artifacts = query
        .artifacts()
        .get_artifacts(&mut store)
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;

    let mut md = format!(
        "# Scatter Plot ([target artifacts]({}))\n",
        query.artifacts().to_url()
    );

    let x_metric_property = if let Some(metric) = &query.x_metric {
        md += &format!("- **X-axis metric property**: {}\n", metric);
        metric
    } else {
        md += "Select x-axis metric property:\n";
        let mut candidates = BTreeSet::new();
        for a in &artifacts {
            for (k, v) in &a.properties {
                if is_int_or_double(v) {
                    candidates.insert(k);
                }
            }
        }
        for c in candidates {
            md += &format!("- [{}]({})\n", c, query.x_metric(c).to_url());
        }
        return Ok(response::markdown(&md));
    };

    let y_metric_property = if let Some(metric) = &query.y_metric {
        md += &format!("- **Y-axis metric property**: {}\n", metric);
        metric
    } else {
        md += "\nSelect y-axis metric property:\n";
        let mut candidates = BTreeSet::new();
        for a in &artifacts {
            for (k, v) in &a.properties {
                if is_int_or_double(v) {
                    candidates.insert(k);
                }
            }
        }
        for c in candidates {
            md += &format!("- [{}]({})\n", c, query.y_metric(c).to_url());
        }
        return Ok(response::markdown(&md));
    };

    if let Some(key) = &query.join_key {
        md += &format!("- **Join key property**: {}\n", key);
    } else {
        let mut candidates = BTreeSet::new();
        for a in &artifacts {
            for (k, v) in &a.properties {
                if is_int(v) {
                    candidates.insert(k);
                }
            }
        }
        if !candidates.is_empty() {
            md += "\nSelect join key property (optional):\n";
            for c in candidates {
                md += &format!("- [{}]({})\n", c, query.join_key(c).to_url());
            }
        }
    };

    if query.x_filter_key.is_none() {
        md += "\nSelect x-axis filter property (optional):\n";
        let mut candidates = BTreeSet::new();
        for a in &artifacts {
            for (k, v) in &a.properties {
                if let (k, Some(v)) = (k, get_string(v)) {
                    candidates.insert((k, v));
                }
            }
        }
        for (k, v) in candidates {
            md += &format!("- [{}] [{}]({})\n", k, v, query.x_filter(k, v).to_url());
        }
    } else {
        md += &format!(
            "- **X-axis filter property**: {}={}\n",
            query.x_filter_key.as_ref().unwrap(),
            query.x_filter_value.as_ref().unwrap()
        );
    }

    if query.y_filter_key.is_none() {
        md += "\nSelect y-axis filter property (optional):\n";
        let mut candidates = BTreeSet::new();
        for a in &artifacts {
            for (k, v) in &a.properties {
                if let (k, Some(v)) = (k, get_string(v)) {
                    candidates.insert((k, v));
                }
            }
        }
        for (k, v) in candidates {
            md += &format!("- [{}] [{}]({})\n", k, v, query.y_filter(k, v).to_url());
        }
    } else {
        md += &format!(
            "- **Y-axis filter property**: {}={}\n",
            query.y_filter_key.as_ref().unwrap(),
            query.y_filter_value.as_ref().unwrap()
        );
    }

    let mut xs = BTreeMap::new();
    for a in &artifacts {
        if let (Some(tk), Some(tv)) = (query.x_filter_key.as_ref(), query.x_filter_value.as_ref()) {
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

        let mut artifact_id = a.id.get();
        if let Some(key) = &query.join_key {
            for (k, v) in &a.properties {
                if k != key {
                    continue;
                }
                if let Some(n) = get_int(v) {
                    artifact_id = n;
                    break;
                }
            }
            // continue; // TODO: error check
        };

        for (k, v) in &a.properties {
            if k != x_metric_property {
                continue;
            }
            if let Some(m) = get_double(v) {
                xs.insert(artifact_id, m);
                break;
            }
        }
    }

    let mut ys = BTreeMap::new();
    for a in &artifacts {
        if let (Some(tk), Some(tv)) = (query.y_filter_key.as_ref(), query.y_filter_value.as_ref()) {
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

        let mut artifact_id = a.id.get();
        if let Some(key) = &query.join_key {
            for (k, v) in &a.properties {
                if k != key {
                    continue;
                }
                if let Some(n) = get_int(v) {
                    artifact_id = n;
                    break;
                }
            }
            // continue;
        }

        for (k, v) in &a.properties {
            if k != y_metric_property {
                continue;
            }
            if let Some(m) = get_double(v) {
                ys.insert(artifact_id, m);
                break;
            }
        }
    }

    let mut xs1 = Vec::new();
    let mut ys1 = Vec::new();
    let mut texts = Vec::new();
    for (id, x) in &xs {
        if let Some(y) = ys.get(id) {
            xs1.push(*x);
            ys1.push(*y);
            texts.push(format!("Artifact: {}", id));
        }
    }

    if !query.do_plot {
        md += &format!("- n_target: {}\n", xs1.len());
        md += &format!("\n**[Do plot]({})**\n", query.do_plot().to_url());
        return Ok(response::markdown(&md));
    }

    let mut plot = Plot::new();
    let trace = Scatter::new(xs1, ys1).mode(Mode::Markers).text_array(texts);
    plot.add_trace(trace);

    let mut html = String::new();
    let mut html_file = tempfile::NamedTempFile::new()?;
    plot.to_html(html_file.path());
    html_file.read_to_string(&mut html)?;
    Ok(response::html(&html))
}
