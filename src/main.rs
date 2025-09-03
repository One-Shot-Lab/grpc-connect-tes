mod accounts;
mod metrics;
mod server;

use crate::accounts::get_accounts;
use crate::metrics::Metrics;
use crate::server::start_metrics_server;
use anyhow::Result;
use clap::Parser;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_stream::StreamExt;
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
use yellowstone_grpc_proto::geyser::{
    CommitmentLevel, SlotStatus, SubscribeRequest, SubscribeRequestFilterSlots,
    SubscribeRequestFilterTransactions, subscribe_update,
};

#[derive(Debug, Clone)]
struct SlotTracker {
    slot: u64,
    creator: String,
    create_ts: u64,
    first_tx_ts: Option<u64>,
    last_tx_ts: Option<u64>,
    current_status: Option<SlotStatus>,
    // Счетчики транзакций по статусам (используем SlotStatus как ключ)
    tx_counts: HashMap<SlotStatus, u64>,
    // Отдельный счетчик транзакций для трекеров, созданных транзакциями
    tx_initiated_count: u64,
}

impl SlotTracker {
    fn new(slot: u64, creator: String) -> Self {
        // println!("Create slot tracker");
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        Self {
            slot,
            creator,
            create_ts: now,
            first_tx_ts: None,
            last_tx_ts: None,
            current_status: None,
            tx_counts: HashMap::new(),
            tx_initiated_count: 0,
        }
    }

    fn apply_transaction(&mut self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // Устанавливаем время первой транзакции, если это первая
        if self.first_tx_ts.is_none() {
            self.first_tx_ts = Some(now);
        }

        self.last_tx_ts = Some(now);

        match self.current_status {
            None => self.tx_initiated_count += 1,
            Some(current_status) => {
                *self.tx_counts.entry(current_status).or_insert(0) += 1;
            }
        }
    }

    fn update_status(&mut self, new_status: SlotStatus) {
        self.current_status = Some(new_status);
    }

    fn print_summary(&self, metrics: &Metrics) {
        let duration_ms = if let Some(last_tx_ts) = self.last_tx_ts {
            last_tx_ts - self.first_tx_ts.unwrap()
        } else {
            0
        };

        let total_txs = self.tx_counts.values().sum::<u64>() + self.tx_initiated_count;

        let mut status_counts = Vec::new();
        if self.tx_initiated_count > 0 {
            status_counts.push(format!("no_status_yet:{}", self.tx_initiated_count));
        }
        for (status, count) in &self.tx_counts {
            if *count > 0 {
                status_counts.push(format!("{:?}:{}", status, count));
            }
        }

        // Обновляем Prometheus метрики
        metrics.record_slot_finalized(duration_ms, self.tx_initiated_count, &self.tx_counts);

        // Выводим в логи для отладки
        println!(
            "FINALIZED slot:{} creator:{} duration:{}ms total_txs:{} tx_by_status:[{}]",
            self.slot,
            self.creator,
            duration_ms,
            total_txs,
            status_counts.join(" ")
        );
    }
}


#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "http://127.0.0.1:10000")]
    endpoint: String,

    #[arg(short, long)]
    x_token: Option<String>,

    #[arg(long, help = "Skip TLS certificate verification (insecure)")]
    insecure: bool,

    #[arg(long, default_value = "9090", help = "Prometheus metrics server port")]
    metrics_port: u16,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize the default crypto provider for rustls
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let args = Args::parse();

    // Создаем Prometheus registry и метрики
    let (metrics, registry) = Metrics::new()?;

    // Запускаем Prometheus metrics server
    let registry_clone = registry.clone();
    let metrics_port = args.metrics_port;
    tokio::spawn(async move {
        start_metrics_server(registry_clone, metrics_port).await;
    });

    println!("Connecting to Yellowstone gRPC endpoint: {}", args.endpoint);

    // Create client using builder pattern
    let mut builder = GeyserGrpcClient::build_from_shared(args.endpoint.clone())?;

    if let Some(token) = args.x_token {
        builder = builder.x_token(Some(token))?;
    }

    // Configure TLS based on endpoint protocol
    let builder = if args.endpoint.starts_with("https://") {
        // For HTTPS endpoints, enable TLS
        let tls_config = ClientTlsConfig::new().with_enabled_roots();

        if args.insecure {
            eprintln!(
                "Warning: --insecure flag specified but certificate verification cannot be disabled"
            );
            eprintln!(
                "If you have certificate issues, try adding the CA certificate to your system trust store"
            );
        }

        builder.tls_config(tls_config)?
    } else {
        builder
    };

    let mut client = builder
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(10))
        .connect()
        .await?;

    println!("Connected successfully!");

    // Subscribe to transactions involving these AMM programs
    let mut transactions = HashMap::new();
    transactions.insert(
        "amm_transactions".to_string(),
        SubscribeRequestFilterTransactions {
            vote: Some(false),
            failed: Some(false),
            signature: None,
            account_include: get_accounts(),
            account_exclude: vec![],
            account_required: vec![],
        },
    );

    // Empty accounts subscription
    let accounts = HashMap::new();

    let mut slots = HashMap::new();
    slots.insert(
        "client".to_string(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: Some(false),
            interslot_updates: Some(true),
        },
    );

    let mut stream = client
        .subscribe_once(SubscribeRequest {
            slots,
            accounts,
            transactions,
            transactions_status: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            entry: HashMap::new(),
            commitment: Some(CommitmentLevel::Confirmed as i32),
            accounts_data_slice: vec![],
            ping: None,
            from_slot: None,
        })
        .await?;

    println!("Listening for updates...");

    // HashMap для отслеживания слотов
    let mut slot_trackers: HashMap<u64, SlotTracker> = HashMap::new();

    while let Some(message) = stream.next().await {
        match message {
            Ok(msg) => {
                match msg.update_oneof {
                    Some(subscribe_update::UpdateOneof::Account(_account)) => {
                        // Account updates are not subscribed to anymore
                    }
                    Some(subscribe_update::UpdateOneof::Slot(slot)) => {
                        let status = SlotStatus::try_from(slot.status)?;
                        if status == SlotStatus::SlotFinalized {
                            if let Some(tracker) = slot_trackers.remove(&slot.slot) {
                                tracker.print_summary(&metrics);
                            }
                        } else {
                            let tracker = slot_trackers.entry(slot.slot).or_insert(
                                SlotTracker::new(slot.slot, format!("slot_update_{:?}", status)),
                            );
                            tracker.update_status(status);
                        }
                    }
                    Some(subscribe_update::UpdateOneof::Transaction(transaction)) => {
                        if let Some(_tx_info) = &transaction.transaction {
                            let tracker =
                                slot_trackers
                                    .entry(transaction.slot)
                                    .or_insert(SlotTracker::new(
                                        transaction.slot,
                                        "transaction".to_string(),
                                    ));
                            tracker.apply_transaction();
                        }
                    }
                    Some(subscribe_update::UpdateOneof::Ping(_)) => {
                        println!("Ping received");
                    }
                    _ => {
                        println!("Other update received");
                    }
                }
            }
            Err(e) => {
                eprintln!("Error receiving message: {}", e);
                break;
            }
        }
    }

    Ok(())
}
