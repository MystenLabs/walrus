// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashMap,
    hash::Hash,
    sync::{Arc, RwLock},
    time::Duration,
};

use prometheus::{
    core::{Collector, Desc},
    proto::{Counter, LabelPair, Metric, MetricFamily, MetricType},
};
use tokio_metrics::TaskMonitor;

use super::Registry;

/// Default duration at which polls cross the threshold into being categorized as 'slow' is 1ms.
///
/// This is currently higher than [`TaskMonitor::DEFAULT_SLOW_POLL_THRESHOLD`] (50 μs) to help
/// identify the really slow tasks.
pub const DEFAULT_SLOW_POLL_THRESHOLD: Duration = Duration::from_millis(1);
/// Default duration at which schedules cross the threshold into being categorized as 'long' is 1ms.
///
/// This is currently higher than [`TaskMonitor::DEFAULT_LONG_DELAY_THRESHOLD`] (50 μs) to help
/// identify the heavily impacted tasks.
pub const DEFAULT_LONG_DELAY_THRESHOLD: Duration = Duration::from_millis(1);

const TASK_METRICS_NAMESPACE: &str = "tokio_task_metrics";

/// A family of [`TaskMonitor`]s.
///
/// Stores a thread-safe map of keys `K` to [`TaskMonitor`]s. Existing or new `TaskMonitor`s can
/// be retrieved with [`Self::get_or_insert_with_task_name`].
#[derive(Debug, Clone)]
pub struct TaskMonitorFamily<K> {
    registry: Registry,
    task_monitors: Arc<RwLock<HashMap<K, TaskMonitor>>>,
}

impl<K> TaskMonitorFamily<K>
where
    K: Hash + Eq,
{
    /// Creates a new instance of the [`TaskMonitorCollector`] and registers it with the provided
    /// prometheus registry.
    pub fn new(registry: Registry) -> Self {
        Self {
            registry,
            task_monitors: Default::default(),
        }
    }

    /// Gets an existing [`TaskMonitor`] with the specified `key`.
    ///
    /// If no such [`TaskMonitor`] exists, then a new one is created under the provided key.
    ///
    /// The function `task_name` is used to provide a name for the task in the associated metrics.
    /// It is attached to the metric as `task = '{task_name()}'` and must be unique among *ALL*
    /// [`TaskMonitor`]s registered on the registry.
    pub fn get_or_insert_with_task_name<F>(&self, key: &K, task_name: F) -> TaskMonitor
    where
        K: Clone,
        F: FnOnce() -> String,
    {
        // In most cases, the route should already be defined so attempt to get it with a read-lock.
        if let Some(task_monitor) = self
            .task_monitors
            .read()
            .expect("mutex is not poisoned")
            .get(key)
            .cloned()
        {
            return task_monitor;
        }

        // Between the above call, and the write-lock below, another thread may have also attempted
        // to create the same monitor. For this reason, when creating the monitor below, fall back
        // to any existing monitor.
        self.task_monitors
            .write()
            .expect("mutex is not poisoned")
            .entry(key.clone())
            .or_insert_with(move || {
                let collector = TaskMonitorCollector::new(task_name());
                let monitor = collector.monitor().clone();
                self.registry
                    .register(Box::new(collector))
                    .expect("collectors must be unique");

                monitor
            })
            .clone()
    }

    /// Gets an existing [`TaskMonitor`] with the specified `key`.
    ///
    /// If no such [`TaskMonitor`] exists, then a new one is created under the provided key with a
    /// task name of `key.to_string()`.
    ///
    /// See [`Self::get_or_insert_with_task_name`] for more details.
    pub fn get_or_insert(&self, key: &K) -> TaskMonitor
    where
        K: Clone + ToString,
    {
        self.get_or_insert_with_task_name(key, || key.to_string())
    }
}

/// Implements the [`prometheus::core::Collector`] interface for the [`TaskMonitor`]
///
/// This allows task metrics to be collected by Prometheus.
///
/// The exported metrics are within the namespace `tokio_task_metrics` and metrics corresponding to
/// durations have the suffix `_seconds` appended to their name and are exported in floating-point
/// seconds.
///
/// Attached to each metric is the constant label `task` which can be specified when
/// creating the `TaskMonitorCollector`.
#[derive(Debug, Clone)]
pub struct TaskMonitorCollector {
    inner: TaskMonitor,
    const_labels: HashMap<String, String>,
    descriptions: Vec<Desc>,
    metric_families: Vec<MetricFamily>,
}

impl Collector for TaskMonitorCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descriptions.iter().collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        self.collect_metrics()
    }
}

impl TaskMonitorCollector {
    /// Create a new instance of [`TaskMonitorCollector`] with the specified task name.
    pub fn new(task: String) -> Self {
        let mut builder = TaskMonitor::builder();
        builder
            .with_long_delay_threshold(DEFAULT_LONG_DELAY_THRESHOLD)
            .with_slow_poll_threshold(DEFAULT_SLOW_POLL_THRESHOLD);

        let const_labels = [("task".to_owned(), task)].into_iter().collect();
        let mut this = Self {
            inner: builder.build(),
            descriptions: vec![],
            metric_families: vec![],
            const_labels,
        };

        this.initialize_descriptions();
        this.initialize_families();
        this
    }

    pub fn monitor(&self) -> &TaskMonitor {
        &self.inner
    }

    /// Initialize the metric families, so that each time metrics are collected, we only need to
    /// clone the families and update their values.
    fn initialize_families(&mut self) {
        assert!(
            !self.descriptions.is_empty(),
            "initialize descriptions first"
        );
        let mut families = Vec::with_capacity(self.descriptions.len());

        for description in &self.descriptions {
            let mut family = MetricFamily::new();
            family.set_name(description.fq_name.clone());
            family.set_help(description.help.clone());

            // We only record accumulating values, so everything is a counter.
            family.set_field_type(MetricType::COUNTER);

            // The only labels attached to each metric are the const labels, which means each
            // family only has a single metric.
            let mut metric = Metric::new();
            let labels = self
                .const_labels
                .iter()
                .map(|(name, value)| to_label_pair(name, value))
                .collect();
            metric.set_label(labels);
            metric.set_counter(Counter::new());

            family.set_metric(vec![metric].into());

            families.push(family);
        }

        self.metric_families = families;
    }
}

macro_rules! convert_metrics {
    (
        $name:ident: [
            $(
                #[help = $help_str:literal]
                $metric_spec:tt
            ),* $(,)?
        ]
    ) => {
        impl $name {
            /// Initialize the descriptions of the metrics to the values that will always be
            /// returned.
            fn initialize_descriptions(
                &mut self,
            )  {
                self.descriptions = vec![
                    $(
                        Desc::new(
                            convert_metrics!(@fq_name $metric_spec),
                            $help_str.to_owned(),
                            vec![],
                            self.const_labels.clone(),
                        )
                        .expect("compile-time defined metric descriptions do not err")
                    ),+
                ];
            }

            fn collect_metrics(&self) -> Vec<MetricFamily> {
                let tokio_metrics = self.inner.cumulative();
                let mut metric_families = self.metric_families.clone();
                let mut index = 0usize;

                $(
                    metric_families[index]
                        .mut_metric()
                        .first_mut()
                        .expect("all families were defined with exactly 1 metric")
                        .mut_counter()
                        .set_value(convert_metrics!(@as_f64 $metric_spec tokio_metrics));

                    // The very last assignment in the unrolled loop is unused.
                    #[allow(unused_assignments)]
                    {
                        index += 1;
                    }
                )+

                metric_families
            }
        }
    };
    (@fq_name ($_original_name:tt -> $name:ident)) => { convert_metrics!(@fq_name $name) };
    (@fq_name $name:ident) => {
        format!("{}_{}", TASK_METRICS_NAMESPACE, stringify!($name))
    };

    (@as_f64 (($name:ident in seconds) -> $_:tt) $tokio_metrics:ident) => {
        ($tokio_metrics.$name).as_secs_f64()
    };
    (@as_f64 $name:ident $tokio_metrics:ident) => {
        ($tokio_metrics.$name) as f64
    };
}

convert_metrics! {
    TaskMonitorCollector: [
        #[help = "Total number of tasks instrumented."]
        instrumented_count,

        #[help = "Total number of tasks that were dropped."]
        dropped_count,

        #[help = "Total number of tasks that were polled for the first time."]
        first_poll_count,

        #[help = "Total duration (in seconds) elapsed between the instant tasks are instrumented \
        and the instant they are first polled."]
        ((total_first_poll_delay in seconds) -> total_first_poll_delay_seconds),

        #[help = "Total number of times that tasks idled, waiting to be awoken"]
        total_idled_count,

        #[help = "Total duration (in seconds) that tasks idled"]
        ((total_idle_duration in seconds) -> total_idle_duration_seconds),

        #[help = "Total number of times that tasks were awoken (and then, presumably, \
        scheduled for execution)"]
        total_scheduled_count,

        #[help = "Total duration (in seconds) that tasks spent waiting to be polled after \
        awakening."]
        ((total_scheduled_duration in seconds) -> total_scheduled_duration_seconds),

        #[help = "Total number of times that tasks were polled."]
        total_poll_count,

        #[help = "Total duration (in seconds) elapsed during polls."]
        ((total_poll_duration in seconds) -> total_poll_duration_seconds),

        #[help = "Total number of times polling tasks completed swiftly."]
        total_fast_poll_count,

        #[help = "Total duration (in seconds) of fast polls."]
        ((total_fast_poll_duration in seconds) -> total_fast_poll_duration_seconds),

        #[help = "Total number of times polling tasks completed slowly."]
        total_slow_poll_count,

        #[help = "Total duration (in seconds) of slow polls."]
        ((total_slow_poll_duration in seconds) -> total_slow_poll_duration_seconds),

        #[help = "Total count of tasks with short scheduling delays."]
        total_short_delay_count,

        #[help = "Total count of tasks with long scheduling delays."]
        total_long_delay_count,

        #[help = "Total duration of tasks with short scheduling delays."]
        ((total_short_delay_duration in seconds) -> total_short_delay_duration_seconds),

        #[help = "Total duration of tasks with long scheduling delays."]
        ((total_long_delay_duration in seconds) -> total_long_delay_duration_seconds),
    ]
}

fn to_label_pair(name: &str, value: &str) -> LabelPair {
    let mut pair = LabelPair::new();
    pair.set_name(name.to_owned());
    pair.set_value(value.to_owned());
    pair
}

#[cfg(test)]
mod test {
    use prometheus::{proto::Counter, Registry};

    use super::*;

    fn task_monitor_collector() -> TaskMonitorCollector {
        TaskMonitorCollector::new("test_monitor".to_owned())
    }

    walrus_test_utils::param_test! {
        descriptions_are_present: [
            instrumented_count: ("tokio_task_metrics_instrumented_count"),
            total_long_delay_duration: ("tokio_task_metrics_total_long_delay_duration_seconds"),
        ]
    }
    fn descriptions_are_present(fq_name: &str) {
        let collector = task_monitor_collector();
        let descriptions = collector.desc();

        let desc = descriptions
            .iter()
            .find(|desc| desc.fq_name == fq_name)
            .expect("description must be present");
        assert_eq!(desc.const_label_pairs[0].get_name(), "task");
        assert_eq!(desc.const_label_pairs[0].get_value(), "test_monitor");
    }

    fn find_by_fqname<'a>(metrics: &'a [MetricFamily], fq_name: &str) -> Option<&'a MetricFamily> {
        metrics.iter().find(|metric| metric.get_name() == fq_name)
    }

    fn as_counter(family: &MetricFamily) -> &Counter {
        family
            .get_metric()
            .first()
            .expect("must have at least one metric in the family")
            .get_counter()
    }

    #[tokio::test]
    async fn sanity_test_counters() {
        let monitor = task_monitor_collector();

        let collected_metrics = monitor.collect();
        let metric_family =
            find_by_fqname(&collected_metrics, "tokio_task_metrics_instrumented_count")
                .expect("metric must exist in collection");

        // 0 tasks have been instrumented
        assert_eq!(as_counter(metric_family).get_value(), 0.0);

        monitor.monitor().instrument(async {});

        let collected_metrics = monitor.collect();
        let metric_family =
            find_by_fqname(&collected_metrics, "tokio_task_metrics_instrumented_count")
                .expect("metric must exist in collection");
        // 1 task has been instrumented
        assert_eq!(as_counter(metric_family).get_value(), 1.0);

        monitor.monitor().instrument(async {});
        monitor.monitor().instrument(async {});

        let collected_metrics = monitor.collect();
        let metric_family =
            find_by_fqname(&collected_metrics, "tokio_task_metrics_instrumented_count")
                .expect("metric must exist in collection");
        // 3 tasks in total have been instrumented
        assert_eq!(as_counter(metric_family).get_value(), 3.0);
    }

    #[test]
    fn registers_successfully() {
        let registry = Registry::default();
        registry
            .register(Box::new(task_monitor_collector()))
            .expect("should successfully register");
    }

    #[test]
    fn monitors_with_different_names_can_both_be_registered() {
        let registry = Registry::default();

        registry
            .register(Box::new(TaskMonitorCollector::new(
                "test_monitor1".to_owned(),
            )))
            .expect("first monitor should successfully register");

        registry
            .register(Box::new(TaskMonitorCollector::new(
                "test_monitor2".to_owned(),
            )))
            .expect("second monitor should successfully register");
    }
}
