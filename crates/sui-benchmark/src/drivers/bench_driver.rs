// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use async_trait::async_trait;
use futures::future::try_join_all;
use futures::future::BoxFuture;
use futures::FutureExt;
use futures::{stream::FuturesUnordered, StreamExt};
use indicatif::ProgressBar;
use indicatif::ProgressStyle;
use prometheus::register_gauge_vec_with_registry;
use prometheus::register_histogram_vec_with_registry;
use prometheus::register_int_counter_vec_with_registry;
use prometheus::GaugeVec;
use prometheus::HistogramVec;
use prometheus::IntCounterVec;
use prometheus::Registry;
use sui_core::authority_aggregator::AuthorityAggregator;
use tokio::sync::OnceCell;

use crate::drivers::driver::Driver;
use crate::drivers::HistogramWrapper;
use crate::workloads::workload::Payload;
use crate::workloads::workload::Workload;
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use sui_core::authority_client::NetworkAuthorityClient;
use sui_quorum_driver::QuorumDriverHandler;
use sui_quorum_driver::QuorumDriverMetrics;
use sui_types::crypto::EmptySignInfo;
use sui_types::messages::{
    ExecuteTransactionRequest, ExecuteTransactionRequestType, ExecuteTransactionResponse,
    TransactionEnvelope,
};
use tokio::sync::Barrier;
use tokio::time;
use tokio::time::Instant;
use tracing::{debug, error};

use super::BenchmarkStats;
use super::Interval;
pub struct BenchMetrics {
    pub num_success: IntCounterVec,
    pub num_error: IntCounterVec,
    pub num_submitted: IntCounterVec,
    pub num_in_flight: GaugeVec,
    pub latency_s: HistogramVec,
}

impl BenchMetrics {
    fn new(registry: &Registry) -> Self {
        BenchMetrics {
            num_success: register_int_counter_vec_with_registry!(
                "num_success",
                "Total number of transaction success",
                &["workload"],
                registry,
            )
            .unwrap(),
            num_error: register_int_counter_vec_with_registry!(
                "num_error",
                "Total number of transaction errors",
                &["workload", "error_type"],
                registry,
            )
            .unwrap(),
            num_submitted: register_int_counter_vec_with_registry!(
                "num_submitted",
                "Total number of transaction submitted to sui",
                &["workload"],
                registry,
            )
            .unwrap(),
            num_in_flight: register_gauge_vec_with_registry!(
                "num_in_flight",
                "Total number of transaction in flight",
                &["workload"],
                registry,
            )
            .unwrap(),
            latency_s: register_histogram_vec_with_registry!(
                "latency_s",
                "Total time in seconds to return a response",
                &["workload"],
                registry,
            )
            .unwrap(),
        }
    }
}

struct Stats {
    pub id: usize,
    pub num_no_gas: u64,
    pub num_submitted: u64,
    pub num_in_flight: u64,
    pub bench_stats: BenchmarkStats,
}

type RetryType = Box<(TransactionEnvelope<EmptySignInfo>, Box<dyn Payload>)>;
enum NextOp {
    Response(Option<(Instant, Box<dyn Payload>)>),
    Retry(RetryType),
}

async fn start_benchmark(pb: Arc<ProgressBar>) -> &'static Instant {
    static ONCE: OnceCell<Instant> = OnceCell::const_new();
    ONCE.get_or_init(|| async move {
        pb.finish_and_clear();
        Instant::now()
    })
    .await
}

pub struct BenchDriver {
    pub num_requests_per_worker: u64,
    pub num_workers: u64,
    pub target_qps: u64,
    pub stat_collection_interval: u64,
    pub start_time: Instant,
}

impl BenchDriver {
    pub fn new(
        target_qps: u64,
        in_flight_ratio: u64,
        num_workers: u64,
        stat_collection_interval: u64,
    ) -> BenchDriver {
        let max_in_flight_ops = target_qps as usize * in_flight_ratio as usize;
        BenchDriver {
            num_requests_per_worker: max_in_flight_ops as u64 / num_workers,
            num_workers,
            target_qps,
            stat_collection_interval,
            start_time: Instant::now(),
        }
    }
    pub fn update_progress(
        start_time: Instant,
        interval: Interval,
        progress_bar: Arc<ProgressBar>,
    ) {
        match interval {
            Interval::Count(count) => {
                progress_bar.inc(1);
                if progress_bar.position() >= count {
                    progress_bar.finish_and_clear();
                }
            }
            Interval::Time(Duration::MAX) => progress_bar.inc(1),
            Interval::Time(duration) => {
                let elapsed_secs = (Instant::now() - start_time).as_secs();
                progress_bar.set_position(std::cmp::min(duration.as_secs(), elapsed_secs));
                if progress_bar.position() >= duration.as_secs() {
                    progress_bar.finish_and_clear();
                }
            }
        }
    }
}

#[async_trait]
impl Driver<BenchmarkStats> for BenchDriver {
    async fn run(
        &self,
        workload: Box<dyn Workload<dyn Payload>>,
        aggregator: AuthorityAggregator<NetworkAuthorityClient>,
        registry: &Registry,
        show_progress: bool,
        run_duration: Interval,
    ) -> Result<BenchmarkStats, anyhow::Error> {
        let mut tasks = Vec::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel(100);
        let request_delay_micros = (1_000_000 * self.num_workers) / self.target_qps;
        let stat_delay_micros = 1_000_000 * self.stat_collection_interval;
        let barrier = Arc::new(Barrier::new(self.num_workers as usize));
        let metrics = Arc::new(BenchMetrics::new(registry));
        let pb = Arc::new(
            ProgressBar::new(self.num_workers)
                .with_prefix("Setting up workers")
                .with_style(
                    ProgressStyle::with_template("{prefix}: {wide_bar} {pos}/{len}").unwrap(),
                ),
        );
        let progress = Arc::new(match run_duration {
            Interval::Count(count) => ProgressBar::new(count)
                .with_prefix("Running benchmark")
                .with_style(
                    ProgressStyle::with_template("{prefix}: {wide_bar} {pos}/{len}").unwrap(),
                ),
            Interval::Time(Duration::MAX) => ProgressBar::new_spinner(),
            Interval::Time(duration) => ProgressBar::new(duration.as_secs())
                .with_prefix("Running benchmark")
                .with_style(
                    ProgressStyle::with_template("{prefix}: {wide_bar} {pos}/{len}").unwrap(),
                ),
        });
        for i in 0..self.num_workers {
            let progress = progress.clone();
            let pb = pb.clone();
            let tx_cloned = tx.clone();
            let cloned_barrier = barrier.clone();
            let metrics_cloned = metrics.clone();
            let mut free_pool = workload
                .make_test_payloads(self.num_requests_per_worker, &aggregator)
                .await;
            pb.inc(1);
            // Make a per worker quorum driver, otherwise they all share the same task.
            let quorum_driver_handler =
                QuorumDriverHandler::new(aggregator.clone(), QuorumDriverMetrics::new_for_tests());
            let qd = quorum_driver_handler.clone_quorum_driver();
            let runner = tokio::spawn(async move {
                cloned_barrier.wait().await;
                let start_time = start_benchmark(pb).await;
                let mut num_success = 0;
                let mut num_error = 0;
                let mut num_no_gas = 0;
                let mut num_in_flight: u64 = 0;
                let mut num_submitted = 0;
                let mut latency_histogram =
                    hdrhistogram::Histogram::<u64>::new_with_max(100000, 2).unwrap();
                let mut request_interval =
                    time::interval(Duration::from_micros(request_delay_micros));
                request_interval.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
                let mut stat_interval = time::interval(Duration::from_micros(stat_delay_micros));
                let mut futures: FuturesUnordered<BoxFuture<NextOp>> = FuturesUnordered::new();

                let mut retry_queue: VecDeque<RetryType> = VecDeque::new();
                let mut stat_start_time: Instant = Instant::now();
                loop {
                    tokio::select! {
                            _ = tokio::signal::ctrl_c() => {
                                break;
                            }
                            _ = stat_interval.tick() => {
                                if tx_cloned
                                    .try_send(Stats {
                                        id: i as usize,
                                        num_no_gas,
                                        num_in_flight,
                                        num_submitted,
                                        bench_stats: BenchmarkStats {
                                            duration: stat_start_time.elapsed(),
                                            num_error,
                                            num_success,
                                            latency_ms: HistogramWrapper {histogram: latency_histogram.clone()},
                                        },
                                    })
                                    .is_err()
                                {
                                    debug!("Failed to update stat!");
                                }
                                num_success = 0;
                                num_error = 0;
                                num_no_gas = 0;
                                num_submitted = 0;
                                stat_start_time = Instant::now();
                                latency_histogram.reset();
                        }
                        _ = request_interval.tick() => {

                            // If a retry is available send that
                            // (sending retries here subjects them to our rate limit)
                            if let Some(b) = retry_queue.pop_front() {
                                num_error += 1;
                                num_submitted += 1;
                                metrics_cloned.num_submitted.with_label_values(&[&b.1.get_workload_type().to_string()]).inc();
                                let metrics_cloned = metrics_cloned.clone();
                                let res = qd
                                    .execute_transaction(ExecuteTransactionRequest {
                                        transaction: b.0.clone(),
                                        request_type: ExecuteTransactionRequestType::WaitForEffectsCert,
                                    })
                                    .map(move |res| {
                                        match res {
                                            Ok(ExecuteTransactionResponse::EffectsCert(result)) => {
                                                let (_, effects) = *result;
                                                let new_version = effects.effects.mutated.iter().find(|(object_ref, _)| {
                                                    object_ref.0 == b.1.get_object_id()
                                                }).map(|x| x.0).unwrap();
                                                NextOp::Response(Some((
                                                    Instant::now(),
                                                    b.1.make_new_payload(new_version, effects.effects.gas_object.0),
                                                ),
                                                ))
                                            }
                                            Ok(resp) => {
                                                error!("unexpected_response: {:?}", resp);
                                                metrics_cloned.num_error.with_label_values(&[&b.1.get_workload_type().to_string(), "unknown_error"]).inc();
                                                NextOp::Retry(b)
                                            }
                                            Err(sui_err) => {
                                                error!("{}", sui_err);
                                                metrics_cloned.num_error.with_label_values(&[&b.1.get_workload_type().to_string(), &sui_err.to_string()]).inc();
                                                NextOp::Retry(b)
                                            }
                                        }
                                    });
                                futures.push(Box::pin(res));
                                continue
                            }

                            // Otherwise send a fresh request
                            if free_pool.is_empty() {
                                num_no_gas += 1;
                            } else {
                                let payload = free_pool.pop().unwrap();
                                num_in_flight += 1;
                                num_submitted += 1;
                                metrics_cloned.num_in_flight.with_label_values(&[&payload.get_workload_type().to_string()]).inc();
                                metrics_cloned.num_submitted.with_label_values(&[&payload.get_workload_type().to_string()]).inc();
                                let tx = payload.make_transaction();
                                let start = Instant::now();
                                let metrics_cloned = metrics_cloned.clone();
                                let res = qd
                                    .execute_transaction(ExecuteTransactionRequest {
                                        transaction: tx.clone(),
                                    request_type: ExecuteTransactionRequestType::WaitForEffectsCert,
                                })
                                .map(move |res| {
                                    match res {
                                        Ok(ExecuteTransactionResponse::EffectsCert(result)) => {
                                            let (_, effects) = *result;
                                            let new_version = effects.effects.mutated.iter().find(|(object_ref, _)| {
                                                object_ref.0 == payload.get_object_id()
                                            }).map(|x| x.0).unwrap();
                                            NextOp::Response(Some((
                                                start,
                                                payload.make_new_payload(new_version, effects.effects.gas_object.0),
                                            )))
                                        }
                                        Ok(resp) => {
                                            error!("unexpected_response: {:?}", resp);
                                            metrics_cloned.num_error.with_label_values(&[&payload.get_workload_type().to_string(), "unknown_error"]).inc();
                                            NextOp::Retry(Box::new((tx, payload)))
                                        }
                                        Err(sui_err) => {
                                            error!("Retry due to error: {}", sui_err);
                                            metrics_cloned.num_error.with_label_values(&[&payload.get_workload_type().to_string(), &sui_err.to_string()]).inc();
                                            NextOp::Retry(Box::new((tx, payload)))
                                        }
                                    }
                                });
                                futures.push(Box::pin(res));
                            }
                        }
                        Some(op) = futures.next() => {
                            match op {
                                NextOp::Retry(b) => {
                                    retry_queue.push_back(b);
                                    BenchDriver::update_progress(*start_time, run_duration, progress.clone());
                                    if progress.is_finished() {
                                        break;
                                    }
                                }
                                NextOp::Response(Some((start, payload))) => {
                                    let latency = start.elapsed();
                                    num_success += 1;
                                    num_in_flight -= 1;
                                    latency_histogram.record(latency.as_millis().try_into().unwrap()).unwrap();
                                    metrics_cloned.latency_s.with_label_values(&[&payload.get_workload_type().to_string()]).observe(latency.as_secs_f64());
                                    metrics_cloned.num_success.with_label_values(&[&payload.get_workload_type().to_string()]).inc();
                                    metrics_cloned.num_in_flight.with_label_values(&[&payload.get_workload_type().to_string()]).dec();
                                    free_pool.push(payload);
                                    BenchDriver::update_progress(*start_time, run_duration, progress.clone());
                                    if progress.is_finished() {
                                        break;
                                    }
                                }
                                NextOp::Response(None) => {
                                    // num_in_flight -= 1;
                                    unreachable!();
                                }
                            }
                        }
                    }
                }
                // send stats one last time
                if tx_cloned
                    .try_send(Stats {
                        id: i as usize,
                        num_no_gas,
                        num_in_flight,
                        num_submitted,
                        bench_stats: BenchmarkStats {
                            duration: stat_start_time.elapsed(),
                            num_error,
                            num_success,
                            latency_ms: HistogramWrapper {
                                histogram: latency_histogram,
                            },
                        },
                    })
                    .is_err()
                {
                    debug!("Failed to update stat!");
                }
            });
            tasks.push(runner);
        }

        let num_workers = self.num_workers;
        let stat_task = tokio::spawn(async move {
            let mut benchmark_stat = BenchmarkStats {
                duration: Duration::ZERO,
                num_error: 0,
                num_success: 0,
                latency_ms: HistogramWrapper {
                    histogram: hdrhistogram::Histogram::<u64>::new_with_max(100000, 2).unwrap(),
                },
            };
            let mut stat_collection: BTreeMap<usize, Stats> = BTreeMap::new();
            let mut counter = 0;
            let mut stat;
            let start = Instant::now();
            while let Some(
                sample_stat @ Stats {
                    id,
                    num_no_gas: _,
                    num_in_flight: _,
                    num_submitted: _,
                    bench_stats: _,
                },
            ) = rx.recv().await
            {
                benchmark_stat.update(start.elapsed(), &sample_stat.bench_stats);
                stat_collection.insert(id, sample_stat);
                let mut total_qps: f32 = 0.0;
                let mut num_success: u64 = 0;
                let mut num_error: u64 = 0;
                let mut latency_histogram =
                    hdrhistogram::Histogram::<u64>::new_with_max(100000, 2).unwrap();
                let mut num_in_flight: u64 = 0;
                let mut num_submitted: u64 = 0;
                let mut num_no_gas = 0;
                for (_, v) in stat_collection.iter() {
                    total_qps +=
                        v.bench_stats.num_success as f32 / v.bench_stats.duration.as_secs() as f32;
                    num_success += v.bench_stats.num_success;
                    num_error += v.bench_stats.num_error;
                    num_no_gas += v.num_no_gas;
                    num_submitted += v.num_submitted;
                    num_in_flight += v.num_in_flight;
                    latency_histogram
                        .add(&v.bench_stats.latency_ms.histogram)
                        .unwrap();
                }
                let denom = num_success + num_error;
                let _error_rate = if denom > 0 {
                    num_error as f32 / denom as f32
                } else {
                    0.0
                };
                counter += 1;
                if counter % num_workers == 0 {
                    stat = format!("Throughput = {}, latency_ms(min/p50/p99/max) = {}/{}/{}/{}, num_success = {}, num_error = {}, no_gas = {}, submitted = {}, in_flight = {}", total_qps, latency_histogram.min(), latency_histogram.value_at_quantile(0.5), latency_histogram.value_at_quantile(0.99), latency_histogram.max(), num_success, num_error, num_no_gas, num_submitted, num_in_flight);
                    if show_progress {
                        eprintln!("{}", stat);
                    }
                }
            }
            benchmark_stat
        });
        drop(tx);
        let _res: Vec<_> = try_join_all(tasks).await.unwrap().into_iter().collect();
        let benchmark_stat = stat_task.await.unwrap();
        Ok(benchmark_stat)
    }
}
