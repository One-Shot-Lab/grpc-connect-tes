use anyhow::Result;
use prometheus::{Counter, Histogram, HistogramOpts, Opts, Registry};
use std::collections::HashMap;
use std::sync::Arc;
use yellowstone_grpc_proto::geyser::SlotStatus;

#[derive(Clone)]
pub struct Metrics {
    pub slot_duration_histogram: Histogram,
    pub tx_by_status_counters: HashMap<String, Counter>,
}

impl Metrics {
    pub fn new() -> Result<(Self, Arc<Registry>)> {
        let registry = Arc::new(Registry::new());
        
        let slot_duration_histogram = Histogram::with_opts(
            HistogramOpts::new(
                "slot_duration_milliseconds",
                "Duration from first to last transaction in a slot (milliseconds)"
            )
            // Оптимизированные buckets для диапазона 1-200мс
            .buckets(vec![
                1.0, 2.0, 5.0, 10.0, 15.0, 25.0, 40.0, 60.0, 80.0, 100.0, 125.0, 150.0, 175.0, 200.0, 250.0, 300.0, 500.0
            ])
        )?;

        // Создаем счетчики для каждого статуса + no_status_yet
        let mut tx_by_status_counters = HashMap::new();
        
        // Добавляем счетчик для транзакций без статуса
        let no_status_counter = Counter::with_opts(
            Opts::new(
                "slot_transactions_no_status_yet",
                "Number of transactions in slots without status yet"
            )
        )?;
        registry.register(Box::new(no_status_counter.clone()))?;
        tx_by_status_counters.insert("no_status_yet".to_string(), no_status_counter);

        // Добавляем счетчики для всех возможных SlotStatus
        let status_variants = [
            (SlotStatus::SlotProcessed, "processed"),
            (SlotStatus::SlotConfirmed, "confirmed"), 
            (SlotStatus::SlotFinalized, "finalized"),
            (SlotStatus::SlotFirstShredReceived, "first_shred_received"),
            (SlotStatus::SlotCompleted, "completed"),
            (SlotStatus::SlotCreatedBank, "created_bank"),
            (SlotStatus::SlotDead, "dead"),
        ];

        for (status, status_name) in status_variants {
            let counter = Counter::with_opts(
                Opts::new(
                    &format!("slot_transactions_{}", status_name),
                    &format!("Number of transactions in {} slots", status_name)
                )
            )?;
            registry.register(Box::new(counter.clone()))?;
            tx_by_status_counters.insert(format!("{:?}", status), counter);
        }

        registry.register(Box::new(slot_duration_histogram.clone()))?;

        let metrics = Metrics {
            slot_duration_histogram,
            tx_by_status_counters,
        };

        Ok((metrics, registry))
    }

    pub fn record_slot_finalized(&self, duration_ms: u64, tx_initiated_count: u64, tx_counts: &HashMap<SlotStatus, u64>) {
        self.slot_duration_histogram.observe(duration_ms as f64);
        
        // Записываем транзакции без статуса
        if tx_initiated_count > 0 {
            if let Some(counter) = self.tx_by_status_counters.get("no_status_yet") {
                counter.inc_by(tx_initiated_count as f64);
            }
        }

        // Записываем транзакции по статусам
        for (status, count) in tx_counts {
            if *count > 0 {
                let status_key = format!("{:?}", status);
                if let Some(counter) = self.tx_by_status_counters.get(&status_key) {
                    counter.inc_by(*count as f64);
                }
            }
        }
    }
}

