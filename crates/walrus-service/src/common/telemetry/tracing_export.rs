// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    env::{self, VarError},
    fmt::Debug,
    fs::File,
    future,
    io::Write,
    ops::{Deref, DerefMut},
    path::PathBuf,
    str::FromStr,
    sync::{Arc, Mutex},
};

use anyhow::{Context as _, Result};
use flate2::{Compression, write::GzEncoder};
use futures::{FutureExt, future::BoxFuture};
use opentelemetry::{
    KeyValue,
    trace::{TraceError, TracerProvider},
};
use opentelemetry_otlp::SpanExporter as OtlpSpanExporter;
use opentelemetry_proto::{
    tonic::collector::trace::v1::ExportTraceServiceRequest,
    transform::{common::tonic::ResourceAttributesWithSchema, trace::tonic},
};
use opentelemetry_sdk::{
    Resource,
    export::trace::{ExportResult, SpanData, SpanExporter},
    runtime,
    trace::{RandomIdGenerator, Sampler, TracerProvider as SdkTracerProvider},
};
use opentelemetry_semantic_conventions::attribute::{SERVICE_NAME, SERVICE_VERSION};
use tokio::task;
use tracing::{Subscriber, level_filters::LevelFilter};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{
    EnvFilter, Layer, layer::SubscriberExt, registry::LookupSpan, util::SubscriberInitExt,
};

use super::SubscriberGuard;

#[derive(Debug)]
enum FileState {
    Init(PathBuf),
    Open(File),
    ClosedDueToFailure,
    ClosedDueToShutdown,
}

/// An exporter that writes traces to a file in OTLP format.
///
/// Each batch of exports are written to the output file as gzipped JSON object. The resulting
/// file, when unzipped, conforms to the format specified for the expeirmental
/// [OpenTelemetry Protocol File Exporter][file-exporter].
///
/// If using a tool like jager, however, it is easiest to just POSTing each line to the OTLP
/// services according to the [OTLP/HTTP][otlphttp] specification.
///
/// [file-exporter]: https://opentelemetry.io/docs/specs/otel/protocol/file-exporter/
/// [otlphttp]: https://opentelemetry.io/docs/specs/otlp/#otlphttp
#[derive(Debug)]
struct FileSpanExporter {
    file: Arc<Mutex<FileState>>,
    resource: Resource,
}

impl FileSpanExporter {
    fn new(path: PathBuf) -> Self {
        Self {
            file: Arc::new(Mutex::new(FileState::Init(path))),
            resource: Resource::default(),
        }
    }

    fn serialize_and_write(
        resource: Resource,
        batch: Vec<SpanData>,
        file: Arc<Mutex<FileState>>,
    ) -> Result<()> {
        // Serialize the data, but don't close the file in case of an error,
        // since this has nothing to do with the file.
        //
        // Furthermore, since the file's state is checked before this method is called,
        // it's unlikely that the work done building the data is wasted, as it should be possible to
        // then write the data.
        let data = build_export_data(&resource, batch)
            .with_context(|| "failed to serialize trace data for export")?;

        let Ok(mut file_guard) = file.lock() else {
            anyhow::bail!("mutex guarding trace output file is poisoned");
        };
        let file_state = file_guard.deref_mut();
        let file = loop {
            match file_state {
                FileState::Open(file) => break file,
                FileState::Init(path) => match File::create(&path) {
                    Ok(file) => {
                        *file_state = FileState::Open(file);
                    }
                    Err(err) => {
                        let result = Err(err).context(format!(
                            "unable to create the file to which to export traces: {}",
                            path.display()
                        ));
                        *file_state = FileState::ClosedDueToFailure;
                        return result;
                    }
                },
                FileState::ClosedDueToFailure | FileState::ClosedDueToShutdown => {
                    anyhow::bail!("file has already been closed")
                }
            }
        };

        if let Err(err) = file.write_all(&data) {
            // Close the file since it's unclear what state the file will be in for subsequent
            // writes. We skip flushing because the file is already corrupted.
            *file_state = FileState::ClosedDueToFailure;
            return Err(err).context("failed to write trace data to the file");
        }

        Ok(())
    }

    fn check_if_file_is_open(&mut self) -> Result<(), TraceError> {
        let file_guard = self.file.lock().expect("mutex not poisoned");

        match file_guard.deref() {
            FileState::ClosedDueToFailure => Err(TraceError::Other(
                anyhow::anyhow!("file has already been closed due to a write error").into(),
            )),
            FileState::ClosedDueToShutdown => Err(TraceError::TracerProviderAlreadyShutdown),
            _ => Ok(()),
        }
    }
}

impl SpanExporter for FileSpanExporter {
    fn export(&mut self, batch: Vec<SpanData>) -> BoxFuture<'static, ExportResult> {
        if let Err(err) = self.check_if_file_is_open() {
            return future::ready(Err(err)).boxed();
        }

        let resource = self.resource.clone();
        let file_state = self.file.clone();

        async {
            match task::spawn_blocking(move || -> anyhow::Result<()> {
                Self::serialize_and_write(resource, batch, file_state)
            })
            .await
            {
                Ok(Ok(())) => Ok(()),
                Ok(Err(err)) => Err(TraceError::Other(err.into())),
                Err(err) => {
                    if let Ok(reason) = err.try_into_panic() {
                        std::panic::resume_unwind(reason);
                    } else {
                        tracing::trace!("export was cancelled");
                        Ok(())
                    }
                }
            }
        }
        .boxed()
    }

    fn shutdown(&mut self) {
        let mut file_guard = self.file.lock().expect("mutex not poisoned");
        let file_state = file_guard.deref_mut();

        if let FileState::Open(file) = file_state
            && let Err(err) = file.flush()
        {
            tracing::error!("failed to flush trace-export file: {:?}", err);
        };

        *file_state = FileState::ClosedDueToShutdown;
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.resource = resource.clone();
    }
}

fn build_export_data(resource: &Resource, spans: Vec<SpanData>) -> anyhow::Result<Vec<u8>> {
    let resource_spans = tonic::group_spans_by_resource_and_scope(
        spans,
        &ResourceAttributesWithSchema::from(resource),
    );

    let mut json_string = serde_json::to_string(&ExportTraceServiceRequest { resource_spans })
        .with_context(|| "failed to serialise traces to JSON")?;

    // Each batch of data will need to be newline terminated in the unzipped data.
    json_string.push('\n');

    // This relies on the fact that the concatenation of gzip streams is a valid gzip stream.
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    let compressed = encoder
        .write_all(&json_string.into_bytes())
        .and_then(|()| encoder.finish())
        .with_context(|| "failed to compress serialized traces")?;

    Ok(compressed)
}

/// The log format to use when exporting traces as logs.
#[derive(Default, Debug)]
pub(crate) enum LogFormat {
    #[default]
    Default,
    Compact,
    Pretty,
    Json,
}

impl FromStr for LogFormat {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let format = match s.to_lowercase().as_str() {
            "default" => LogFormat::Default,
            "compact" => LogFormat::Compact,
            "pretty" => LogFormat::Pretty,
            "json" => LogFormat::Json,
            _ => anyhow::bail!("invalid log format '{}'", s),
        };
        Ok(format)
    }
}

#[derive(Debug)]
enum TracesExporterChoice {
    Otlp,
    File(PathBuf),
}

#[derive(Debug, Default)]
pub(crate) struct TracingSubscriberBuilder {
    default_log_format: Option<LogFormat>,
    trace_exporter: Option<TracesExporterChoice>,
}

impl TracingSubscriberBuilder {
    /// Export traces to an OTLP collector.
    pub fn with_otlp_trace_exporter(&mut self) -> &mut Self {
        self.trace_exporter = Some(TracesExporterChoice::Otlp);
        self
    }

    /// Export traces to the specified file.
    pub fn with_file_trace_exporter(&mut self, path: PathBuf) -> &mut Self {
        self.trace_exporter = Some(TracesExporterChoice::File(path));
        self
    }

    fn build_log_layer<S>(&mut self) -> Result<impl Layer<S> + 'static>
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        let env_format = match env::var("LOG_FORMAT") {
            Ok(value) => Some(LogFormat::from_str(&value).context("could not parse LOG_FORMAT")?),
            Err(VarError::NotPresent) => None,
            Err(VarError::NotUnicode(_)) => anyhow::bail!("LOG_FORMAT is not valid unicode"),
        };

        let layer = tracing_subscriber::fmt::layer().with_writer(std::io::stderr);
        let layer = match env_format.or(self.default_log_format.take()) {
            Some(LogFormat::Json) => layer.json().boxed(),
            Some(LogFormat::Compact) => layer.compact().boxed(),
            Some(LogFormat::Pretty) => layer.pretty().boxed(),
            None | Some(LogFormat::Default) => layer.boxed(),
        };

        let filter = EnvFilter::new(format!(
            "info,{}",
            env::var(EnvFilter::DEFAULT_ENV).unwrap_or_default()
        ));

        Ok(layer.with_filter(filter))
    }

    fn build_trace_layer<S>(
        exporter: TracesExporterChoice,
    ) -> Result<(impl Layer<S>, SdkTracerProvider)>
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        let builder = SdkTracerProvider::builder()
            .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
                1.0,
            ))))
            .with_id_generator(RandomIdGenerator::default())
            .with_resource(resource());

        let builder = match exporter {
            TracesExporterChoice::Otlp => {
                let batch_exporter = OtlpSpanExporter::builder()
                    .with_tonic()
                    .build()
                    .expect("building the OTLP exporter during startup should not fail");
                builder.with_batch_exporter(batch_exporter, runtime::Tokio)
            }
            TracesExporterChoice::File(path) => {
                builder.with_batch_exporter(FileSpanExporter::new(path), runtime::Tokio)
            }
        };

        let tracer_provider = builder.build();
        let tracer = tracer_provider.tracer("tracing-otel-subscriber");
        let filter = EnvFilter::builder()
            .with_default_directive(LevelFilter::INFO.into())
            .with_env_var("TRACE_FILTER")
            .from_env()
            .context("failed to parse the TRACE_FILTER environment variable")?;

        Ok((
            OpenTelemetryLayer::new(tracer).with_filter(filter),
            tracer_provider,
        ))
    }

    fn build_subscriber(
        &mut self,
    ) -> Result<(impl Subscriber + SubscriberInitExt, SubscriberGuard)> {
        let trace_exporter = self.trace_exporter.take();

        let (trace_layer, tracer_provider) = trace_exporter
            .map(Self::build_trace_layer)
            .transpose()
            .context("failed to initialise tracing export layer")?
            .unzip();

        let log_layer = self
            .build_log_layer()
            .context("failed to initialise logging layer")?;

        let subscriber = tracing_subscriber::registry()
            .with(log_layer)
            .with(trace_layer);

        let guard = tracer_provider
            .map(SubscriberGuard::otlp)
            .unwrap_or_default();

        Ok((subscriber, guard))
    }

    /// Initializes the logger and tracing subscriber as the global subscriber.
    pub fn init(&mut self) -> Result<SubscriberGuard> {
        let (subscriber, guard) = self.build_subscriber()?;
        subscriber.init();
        tracing::debug!("initialized global tracing subscriber");
        Ok(guard)
    }

    /// Initializes the logger and tracing subscriber as the global subscriber,
    /// which is reset once the provided guard is dropped.
    pub fn init_scoped(&mut self) -> Result<SubscriberGuard> {
        let (subscriber, mut guard) = self.build_subscriber()?;
        let default_guard = subscriber.set_default();
        guard = guard.with_default_guard(default_guard);
        tracing::debug!("initialized scoped tracing subscriber");
        Ok(guard)
    }
}

fn resource() -> Resource {
    Resource::new([
        KeyValue::new(SERVICE_VERSION, env!("CARGO_PKG_VERSION")),
        KeyValue::new(SERVICE_NAME, "walrus-cli"),
    ])
}
