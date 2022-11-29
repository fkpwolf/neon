// ======================================================================
// Billing metrics
// ======================================================================

use std::sync::Arc;

use anyhow;
use tokio::sync::Mutex;
use tracing::*;
use utils::id::TimelineId;

use crate::task_mgr;
use crate::tenant_mgr;
use pageserver_api::models::TenantState;
use utils::id::TenantId;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Utc};

// BillingMetric struct that defines the format for one metric entry
// i.e.
//
// {
// "metric": "s3_tenant_size_bytes",
// "type": "absolute",
// "tenant_id": "5d07d9ce9237c4cd845ea7918c0afa7d",
// "timeline_id": "00000000000000000000000000000000",
// "time": ...,
// "value": 12345454,
// }
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct BillingMetric {
    pub metric: BillingMetricKind,
    pub metric_type: String,
    pub tenant_id: TenantId,
    pub timeline_id: Option<TimelineId>,
    pub time: DateTime<Utc>,
    pub value: u64,
}

impl BillingMetric {
    pub fn new_absolute(
        metric: BillingMetricKind,
        tenant_id: TenantId,
        timeline_id: Option<TimelineId>,
        value: u64,
    ) -> Self {
        Self {
            metric,
            metric_type: "absolute".to_string(),
            tenant_id,
            timeline_id,
            time: Utc::now(),
            value,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BillingMetricKind {
    // Amount of WAL produced , by a timeline, i.e. last_record_lsn
    // This is an absolute, per-timeline metric.
    WrittenSize,
    // Size of all tenant branches including WAL
    // This is an absolute, per-tenant metric.
    // This is the metric that tenant/tenant_id/size endpoint returns.
    // TODO come up with better name
    SyntheticStorageSize,
    // Size of all the files in the tenant's directory on disk on the pageserver.
    // This is an absolute, per-tenant metric.
    // See also prometheus metric CURRENT_PHYSICAL_SIZE.
    PhysicalSize,
    // Size of the remote storage (S3) directory.
    // This is an absolute, per-tenant metric.
    // Currently the same as PhysicalSize, but that will change when we
    // implement on-demand download.
    S3StorageSize,
}

impl FromStr for BillingMetricKind {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "written_size" => Ok(Self::WrittenSize),
            "synthetic_storage_size" => Ok(Self::SyntheticStorageSize),
            "physical_size" => Ok(Self::PhysicalSize),
            "s3_storage_size" => Ok(Self::S3StorageSize),
            _ => anyhow::bail!("invalid value \"{s}\" for metric type"),
        }
    }
}

impl fmt::Display for BillingMetricKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            BillingMetricKind::WrittenSize => "written_size",
            BillingMetricKind::SyntheticStorageSize => "synthetic_storage_size",
            BillingMetricKind::PhysicalSize => "physical_size",
            BillingMetricKind::S3StorageSize => "s3_storage_size",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BillingMetricsKey {
    tenant_id: TenantId,
    timeline_id: Option<TimelineId>,
    metric: BillingMetricKind,
}

#[derive(serde::Serialize)]
struct EventChunk<'a> {
    events: &'a [BillingMetric],
}

// Main thread that serves metrics collection
pub async fn collect_metrics(conf: &crate::config::PageServerConf) -> anyhow::Result<()> {
    let mut ticker = tokio::time::interval(conf.metric_collection_interval);

    info!("starting collect_metrics");

    // define client here to reuse it for all requests
    let client = reqwest::Client::new();
    let cached_metrics: Arc<Mutex<HashMap<BillingMetricsKey, u64>>> =
        Arc::new(Mutex::new(HashMap::new()));

    loop {
        tokio::select! {
            _ = task_mgr::shutdown_watcher() => {
                info!("collect_metrics received cancellation request");
                return Ok(());
            },
            _ = ticker.tick() => {
                let cached_metrics = cached_metrics.clone();
                collect_metrics_task(&client, &cached_metrics, &conf.metric_collection_endpoint).await?;
            }
        }
    }
}

// One iteration of metrics collection
// Gather per-tenant and per-timeline metrics and send them to billing service,
// cache them to avoid sending the same metrics multiple times.
pub async fn collect_metrics_task(
    client: &reqwest::Client,
    cached_metrics: &Arc<Mutex<HashMap<BillingMetricsKey, u64>>>,
    metric_collection_endpoint: &str,
) -> anyhow::Result<()> {
    let mut current_metrics: Vec<(BillingMetricsKey, u64)> = Vec::new();
    trace!("starting collect_metrics_task");

    // get list of tenants
    let tenants = tenant_mgr::list_tenants().await;

    // iterate through list of Active tenants and collect metrics
    for (tenant_id, tenant_state) in tenants {
        if tenant_state != TenantState::Active {
            continue;
        }

        let tenant = tenant_mgr::get_tenant(tenant_id, true).await?;

        let mut tenant_physical_size = 0;

        // iterate through list of timelines in tenant
        for timeline in tenant.list_timelines().iter() {
            let timeline_written_size = u64::from(timeline.get_last_record_lsn());

            current_metrics.push((
                BillingMetricsKey {
                    tenant_id,
                    timeline_id: Some(timeline.timeline_id),
                    metric: BillingMetricKind::WrittenSize,
                },
                timeline_written_size,
            ));

            let timeline_size = timeline.get_physical_size();
            tenant_physical_size += timeline_size;

            debug!(
                "per-timeline current metrics for tenant: {}: timeline {} physical_size={} last_record_lsn {} (as bytes)",
                tenant_id, timeline.timeline_id, timeline_size, timeline_written_size)
        }

        let tenant_remote_size = tenant.get_remote_size().await?;
        debug!(
            "collected current metrics for tenant: {}: state={:?} tenant_physical_size={} remote_size={}",
            tenant_id, tenant_state, tenant_physical_size, tenant_remote_size
        );

        current_metrics.push((
            BillingMetricsKey {
                tenant_id,
                timeline_id: None,
                metric: BillingMetricKind::PhysicalSize,
            },
            tenant_physical_size,
        ));

        current_metrics.push((
            BillingMetricsKey {
                tenant_id,
                timeline_id: None,
                metric: BillingMetricKind::S3StorageSize,
            },
            tenant_remote_size,
        ));

        // TODO add SyntheticStorageSize metric
    }

    // Filter metrics
    let mut cached_metrics_guard = cached_metrics.lock().await;
    let mut filtered_metrics = Vec::new();

    for (curr_key, curr_val) in current_metrics.iter() {
        if let Some(val) = cached_metrics_guard.insert(curr_key.clone(), *curr_val) {
            if val != *curr_val {
                // metric was updated, send it
                filtered_metrics.push({
                    BillingMetric::new_absolute(
                        curr_key.metric,
                        curr_key.tenant_id,
                        curr_key.timeline_id,
                        *curr_val,
                    )
                });
            }
        } else {
            // cache the metric
            cached_metrics_guard.insert(curr_key.clone(), *curr_val);
            filtered_metrics.push({
                BillingMetric::new_absolute(
                    curr_key.metric,
                    curr_key.tenant_id,
                    curr_key.timeline_id,
                    *curr_val,
                )
            });
        }
    }

    if filtered_metrics.is_empty() {
        trace!("no new metrics to send");
        return Ok(());
    }

    // Send metrics to billing service.
    // split into chunks of 1000 metrics to avoid exceeding the max request size
    const CHUNK_SIZE: usize = 1000;
    let chunks = filtered_metrics.chunks(CHUNK_SIZE);

    for chunk in chunks {
        let chunk_json = serde_json::value::to_raw_value(&EventChunk { events: chunk })
            .expect("BillingMetric should not fail serialization");

        let res = client
            .post(metric_collection_endpoint)
            .json(&chunk_json)
            .send()
            .await;

        match res {
            Ok(res) => {
                if res.status().is_success() {
                    debug!("metrics sent successfully, response: {:?}", res);
                } else {
                    error!("failed to send metrics: {:?}", res);
                }
            }
            Err(err) => {
                error!("failed to send metrics: {:?}", err);
            }
        }
    }

    Ok(())
}
