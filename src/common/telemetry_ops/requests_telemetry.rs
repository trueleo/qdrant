use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use segment::common::operation_time_statistics::{
    OperationDurationStatistics, OperationDurationsAggregator, ScopeDurationMeasurer,
};
use serde::{Deserialize, Serialize};

pub type HttpStatusCode = u16;

#[derive(PartialEq, Eq, Hash, Clone, Debug, Serialize, Deserialize)]
pub struct WebApiTelemetryKey {
    pub request_key: String,
    pub status_code: u16,
    pub collection: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Default, Debug, JsonSchema)]
pub struct WebApiTelemetry {
    pub responses: HashMap<WebApiTelemetryKey, OperationDurationStatistics>,
}

#[derive(Serialize, Deserialize, Clone, Default, Debug, JsonSchema)]
pub struct GrpcTelemetry {
    pub responses: HashMap<String, OperationDurationStatistics>,
}

pub struct ActixTelemetryCollector {
    pub workers: Vec<Arc<Mutex<ActixWorkerTelemetryCollector>>>,
}

#[derive(Default)]
pub struct ActixWorkerTelemetryCollector {
    methods: HashMap<WebApiTelemetryKey, Arc<Mutex<OperationDurationsAggregator>>>,
}

pub struct TonicTelemetryCollector {
    pub workers: Vec<Arc<Mutex<TonicWorkerTelemetryCollector>>>,
}

#[derive(Default)]
pub struct TonicWorkerTelemetryCollector {
    methods: HashMap<String, Arc<Mutex<OperationDurationsAggregator>>>,
}

impl ActixTelemetryCollector {
    pub fn create_web_worker_telemetry(&mut self) -> Arc<Mutex<ActixWorkerTelemetryCollector>> {
        let worker: Arc<Mutex<_>> = Default::default();
        self.workers.push(worker.clone());
        worker
    }

    pub fn get_telemetry_data(&self) -> WebApiTelemetry {
        let mut result = WebApiTelemetry::default();
        for web_data in &self.workers {
            let lock = web_data.lock().get_telemetry_data();
            result.merge(&lock);
        }
        result
    }
}

impl TonicTelemetryCollector {
    #[allow(dead_code)]
    pub fn create_grpc_telemetry_collector(&mut self) -> Arc<Mutex<TonicWorkerTelemetryCollector>> {
        let worker: Arc<Mutex<_>> = Default::default();
        self.workers.push(worker.clone());
        worker
    }

    pub fn get_telemetry_data(&self) -> GrpcTelemetry {
        let mut result = GrpcTelemetry::default();
        for grpc_data in &self.workers {
            let lock = grpc_data.lock().get_telemetry_data();
            result.merge(&lock);
        }
        result
    }
}

impl TonicWorkerTelemetryCollector {
    #[allow(dead_code)]
    pub fn add_response(&mut self, method: String, instant: std::time::Instant) {
        let aggregator = self
            .methods
            .entry(method)
            .or_insert_with(OperationDurationsAggregator::new);
        ScopeDurationMeasurer::new_with_instant(aggregator, instant);
    }

    pub fn get_telemetry_data(&self) -> GrpcTelemetry {
        let mut responses = HashMap::new();
        for (method, aggregator) in self.methods.iter() {
            responses.insert(method.clone(), aggregator.lock().get_statistics());
        }
        GrpcTelemetry { responses }
    }
}

impl ActixWorkerTelemetryCollector {
    pub fn add_response(
        &mut self,
        request_key: String,
        status_code: HttpStatusCode,
        collection: Option<String>,
        instant: std::time::Instant,
    ) {
        let aggregator = self
            .methods
            .entry(WebApiTelemetryKey {
                request_key,
                collection,
                status_code,
            })
            .or_insert_with(OperationDurationsAggregator::new);
        ScopeDurationMeasurer::new_with_instant(aggregator, instant);
    }

    pub fn get_telemetry_data(&self) -> WebApiTelemetry {
        let mut responses = HashMap::new();
        for (key, aggregator) in &self.methods {
            responses.insert(key.clone(), aggregator.lock().get_statistics());
        }
        WebApiTelemetry { responses }
    }
}

impl GrpcTelemetry {
    pub fn merge(&mut self, other: &GrpcTelemetry) {
        for (method, other_statistics) in &other.responses {
            let entry = self.responses.entry(method.clone()).or_default();
            *entry = entry.clone() + other_statistics.clone();
        }
    }
}

impl WebApiTelemetry {
    pub fn merge(&mut self, other: &WebApiTelemetry) {
        for (key, statistics) in &other.responses {
            if let Some(entry) = self.responses.get_mut(key) {
                *entry = entry.clone() + statistics.clone();
            } else {
                self.responses.insert(key.clone(), statistics.clone());
            }
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct RequestsTelemetry {
    pub rest: WebApiTelemetry,
    pub grpc: GrpcTelemetry,
}

impl RequestsTelemetry {
    pub fn collect(
        actix_collector: &ActixTelemetryCollector,
        tonic_collector: &TonicTelemetryCollector,
    ) -> Self {
        let rest = actix_collector.get_telemetry_data();
        let grpc = tonic_collector.get_telemetry_data();
        Self { rest, grpc }
    }
}

impl Anonymize for RequestsTelemetry {
    fn anonymize(&self) -> Self {
        let rest = self.rest.anonymize();
        let grpc = self.grpc.anonymize();
        Self { rest, grpc }
    }
}

impl Anonymize for WebApiTelemetry {
    fn anonymize(&self) -> Self {
        let mut res = self.clone();
        res.responses.values_mut().for_each(|value| {
            value.anonymize();
        });
        res
    }
}

impl Anonymize for GrpcTelemetry {
    fn anonymize(&self) -> Self {
        let responses = self
            .responses
            .iter()
            .map(|(key, value)| (key.clone(), value.anonymize()))
            .collect();

        GrpcTelemetry { responses }
    }
}
