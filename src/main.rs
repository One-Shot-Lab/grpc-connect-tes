use anyhow::Result;
use clap::Parser;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_stream::StreamExt;
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
use yellowstone_grpc_proto::geyser::{
    CommitmentLevel, SlotStatus, SubscribeRequest, 
    SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions, subscribe_update,
};

#[derive(Debug, Clone)]
struct SlotTracker {
    creator: String,
    create_ts: u64,
    first_tx_ts: Option<u64>,
    last_tx_ts: u64,
    current_status: SlotStatus,
    // Счетчики транзакций по статусам (используем SlotStatus как ключ)
    tx_counts: HashMap<SlotStatus, u64>,
    // Отдельный счетчик транзакций для трекеров, созданных транзакциями
    tx_initiated_count: u64,
}

impl SlotTracker {
    fn new(creator: String, status: SlotStatus) -> Self {
        // println!("Create slot tracker");
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        
        Self {
            creator,
            create_ts: now,
            first_tx_ts: None,
            last_tx_ts: now,
            current_status: status,
            tx_counts: HashMap::new(),
            tx_initiated_count: 0,
        }
    }
    
    fn new_from_transaction(creator: String) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        
        // Используем статус 0 (Processed) как дефолтный
        let default_status = SlotStatus::try_from(0).unwrap_or(SlotStatus::default());
        
        Self {
            creator,
            create_ts: now,
            first_tx_ts: Some(now), // Устанавливаем время первой транзакции
            last_tx_ts: now,
            current_status: default_status,
            tx_counts: HashMap::new(),
            tx_initiated_count: 1, // Начинаем с 1 транзакции
        }
    }
    
    fn update_transaction(&mut self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        
        // Устанавливаем время первой транзакции, если это первая
        if self.first_tx_ts.is_none() {
            self.first_tx_ts = Some(now);
        }
        
        self.last_tx_ts = now;
        
        // Если трекер создан транзакцией, увеличиваем tx_initiated_count
        if self.creator == "transaction" {
            self.tx_initiated_count += 1;
        } else {
            // Увеличиваем счетчик для текущего статуса (для трекеров, созданных слотом)
            *self.tx_counts.entry(self.current_status).or_insert(0) += 1;
        }
    }
    
    fn update_status(&mut self, new_status: SlotStatus) {
        self.current_status = new_status;
    }
    
    fn print_summary(&self, slot: u64) {
        // Считаем время от первой транзакции, если есть, иначе от создания трекера
        let duration = if let Some(first_tx) = self.first_tx_ts {
            self.last_tx_ts - first_tx
        } else {
            self.last_tx_ts - self.create_ts
        };
        
        let total_txs: u64 = if self.creator == "transaction" {
            self.tx_initiated_count
        } else {
            self.tx_counts.values().sum()
        };
        
        if self.creator == "transaction" {
            // Для трекеров, созданных транзакциями
            println!("FINALIZED slot:{} creator:{} tx_duration:{}ms tx_initiated:{}", 
                slot,
                self.creator,
                duration,
                self.tx_initiated_count
            );
        } else {
            // Для трекеров, созданных слотами - показываем разбивку по статусам
            let mut status_counts = Vec::new();
            for (status, count) in &self.tx_counts {
                if *count > 0 {
                    status_counts.push(format!("{:?}:{}", status, count));
                }
            }
            
            let status_details = if status_counts.is_empty() {
                "no_transactions".to_string()
            } else {
                status_counts.join(" ")
            };
            
            let tx_duration_info = if self.first_tx_ts.is_some() {
                format!(" tx_duration:{}ms", duration)
            } else {
                " tx_duration:0ms".to_string()
            };
            
            println!("FINALIZED slot:{} creator:{}{} total_txs:{} status_breakdown:[{}]", 
                slot,
                self.creator,
                tx_duration_info,
                total_txs,
                status_details
            );
        }
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
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize the default crypto provider for rustls
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    
    let args = Args::parse();

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
            eprintln!("Warning: --insecure flag specified but certificate verification cannot be disabled");
            eprintln!("If you have certificate issues, try adding the CA certificate to your system trust store");
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

    // Subscribe to AMM account updates
    let unite_amm_array = [
        ("dp2waEWSBy5yKmq65ergoU3G6qRLmqa6K7We4rZSKph", "dx", "Dradex"),
        ("7WduLbRfYhTJktjLw5FDEyrqoEv61aTTCuGAetgLjzN5", "gz", "GooseFX"),
        ("cysPXAjehMpVKUapzbMCCnpFxUFFryEWEaLgnb9NrR8", "ck", "Cykura"),
        ("EewxydAPCCVuNEyrVN68PuSYdQ7wKn27V9Gjeoi8dy3S", "ln", "Lifinity"),
        ("C1onEW2kPetmHmwe74YC1ESx3LnFEpVau6g2pg4fHycr", "cn", "Clone"),
        ("D3BBjqUdCYuP18fNvvMbPAZ8DpcRi4io2EsYHQawJDag", "bs", "sentre"),
        ("GFXsSL5sSaDfNFQUYsHekbWBW1TsFdjDYzACh62tEHxn", "g2", "GooseFX v2"),
        ("HyhpEq587ANShDdbx1mP4dTmDZC44CXWft29oYQXDb53", "Fx", "Fox"),
        ("DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1", "o1", "Orca v1"),
        ("9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP", "o2", "Orca"),
        ("MERLuDFBMmsHnsBPZw2sDQZHvXFMwp8EdjudcU2HKky", "mr", "Mercurial"),
        ("9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin", "rm", "Serum"),
        ("SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ", "br", "Saber"),
        ("PSwapMdSai8tjrEXcxFeQth87xC4rRsa4VA5mhGhXkP", "pg", "Penguin"),
        ("AMM55ShdkoGRB5jVYPjWziwk8m5MpwyDgsMWHaMSQWH6", "a1", "Aldrin"),
        ("CURVGoZn8zycx6FXwwevgBTB2gVvdbGTEpvMJDbgs2t4", "a2", "Aldrin v2"),
        ("SSwpMgqNDsyV7mAgN9ady4bDVu5ySjmmXejXvy2vLt1", "tp", "Step"),
        ("CTMAxxk34HjKWxQ3QLZK1HpaLXmBveao3ESePXbiyfzh", "cp", "Cropper"),
        ("SCHAtsf8mbjyjiv4LkhLKutTf6JnZAbdJKFkXQNMFHZ", "nh", "Sencha"),
        ("CLMM9tUoggJu2wagPkkqs9eFG4BWhVBZWkP1qv3Sp7tR", "cm", "Crema"),
        ("SSwapUtytfBdBn1b9NUGG6foMVPtcWgpRU32HToDUZr", "ss", "Saros"),
        ("MarBmsSgKXdrN1egZf5sqe1TMai9K1rChYNDJgjq7aD", "md", "Marinade"),
        ("Dooar9JkhdZ7J3LHN3A7YCuoGRUggXhQaG4kijfLGU2j", "pn", "Stepn"),
        ("HyaB3W9q6XdA5xwpU4XnSZV94htfmbmqJXZcEbRaJutt", "iv", "Invariant"),
        ("DecZY86MU5Gj7kppfUCEmd4LbXXuyZH1yHaP2NTqdiZB", "dw", "Saber Decimal Wrapper"),
        ("srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX", "O", "Openbook"),
        ("9tKE7Mbmj4mxDjWatikzGAtkoWosiiZX9y6J4Hfm2R8H", "mp", "Marco Polo"),
        ("2KehYt3KsEQR53jYcxjbQp2d2kCp4AkuQW68atufRwSr", "ym", "Symmetry"),
        ("BSwp6bEBihVLdqJRKGgzjcGLHkcTuzmSo1TQkHepzH8p", "bk", "BonkSwap"),
        ("treaf4wWBBty3fHdyBpo35Mz84M8k3heKXmjmi9vFt5", "hn", "Helium Network"),
        ("stkitrT1Uoy18Dk1fTrgPw8W6MVzoCfYoAFT4MLsmhq", "ut", "unstake.it"),
        ("SwaPpA9LAaLfeLi3a68M4DjnLqgtticKg6CnyNwgAC8", "TS", "Token Swap"),
        ("DSwpgjMvXhtGn6BsbqmacdBZyfLj6jSWf3HJpdJtmg6N", "dl", "Dexlab"),
        ("H8W3ctz92svYg6mkn1UtGfu2aQr2fnUFHM1RhScEtQDt", "CW", "Cropper Whirlpool"),
        ("5ocnV1qiCgaQR8Jb8xWnVbApfaygJ8tNoZfgPwsgx9kx", "Sn", "Sanctum S"),
        ("Gswppe6ERWKpUTXvRPfXdzHhiCyJvLadVvXGfdpBqcE1", "GS", "GuacSwap"),
        ("DEXYosS6oEGvk8uCDayvwEZz4qEyDJRf9nFgYCaqPMTm", "1x", "1DEX"),
        ("PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu", "ps", "Perps"),
        ("obriQD1zbpyLz95G5n7nJe6a4DPjpFwa5XYPoNm113y", "ob", "Obric"),
        ("FLUXubRmkEi2q6K3Y9kBPg9248ggaZVsoSFhtJHSrm1X", "FB", "FluxBeam"),
        ("Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB", "M", "Meteora"),
        ("2wT8Yq49kHgDzXuPxZSaeLaH1qbmGXtEyPy64bL7aD3c", "L2", "Lifinity v2"),
        ("LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo", "MD", "Meteora DLMM"),
        ("CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK", "RC", "Raydium CLMM"),
        ("whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc", "W", "Whirlpool"),
        ("SoLFiHG9TfgtdUXUjWAxi3LtvYuFyDLVhBWxdMZxyCe", "SF", "SolFi"),
        ("CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C", "RP", "Raydium CP"),
        ("opnb2LAfJYbRMAHHvqjCwQxanZn7ReEHp1k81EohpZb", "O2", "Openbook v2"),
        ("PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY", "Ph", "Phoenix"),
        ("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P", "Pp", "Pump.Fun"),
        ("JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4", "JUP", "jup v6"),
        ("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8", "R", "Raydium"),
        ("MR2LqxoSbw831bNy68utpu5n4YqBH3AzDmddkgk9LQv", "ms", "Marinade stacking"),
        ("swapNyd8XiQwJ6ianp9snpu4brUqFxadzvHebnAXjJZ", "st", "StableSwap"),
        ("swapFpHZwjELNnjvThjajtiVmkz3yPQEHjLtka2fwHW", "sw", "StableWeighted"),
        ("5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h", "Rl", "Raydium Liquid"),
        ("ZERor4xhbUycZ6gb9ntrhqscUcZmAbQDjEAtCf4hbZY", "Z", "ZeroFi"),
        ("MoonCVVNZFSYkqNXP6bxHLPL6QQJiMagDL3qcqUQTrG", "mn", "Moonshot"),
        ("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA", "pA", "PumpFun new"),
        ("GAMMA7meSFWaBXF25oSUgmGRwaW6sCMFLmBNiMSdbHVT", "Gm", "Gamma amm"),
        ("NUMERUNsFCP3kuNmWZuXtm1AaQCPj9uw6Guv2Ekoi5P", "nm", "numeraire"),
        ("1MooN32fuBBgApc8ujknKJw5sef3BVwPGgz3pto1BAh", "MU", "new moon"),
        ("WooFif76YGRNjk1pA8wCsN67aQsD9f9iLsz4NcJ1AVb", "wf", "woofie"),
        ("LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj", "lp", "Launchpad Ray"),
    ];
    
    // Collect all AMM program IDs
    let amm_pubkeys: Vec<String> = unite_amm_array.iter()
        .map(|(program_id, _, _)| program_id.to_string())
        .collect();
    
    // Subscribe to transactions involving these AMM programs
    let mut transactions = HashMap::new();
    transactions.insert("amm_transactions".to_string(), SubscribeRequestFilterTransactions {
        vote: Some(false),
        failed: Some(false),
        signature: None,
        account_include: amm_pubkeys,
        account_exclude: vec![],
        account_required: vec![],
    });
    
    // Empty accounts subscription
    let accounts = HashMap::new();
    
    println!("Subscribing to transactions for {} AMM programs...", unite_amm_array.len());

    let mut slots = HashMap::new();
    slots.insert("client".to_string(), SubscribeRequestFilterSlots {
        filter_by_commitment: Some(false),
        interslot_updates: Some(true),
    });

    let mut stream = client.subscribe_once(SubscribeRequest {
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
    }).await?;

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
                        if let Ok(status) = SlotStatus::try_from(slot.status) {
                            let slot_num = slot.slot;
                            
                            // Проверяем, есть ли уже такой слот в отслеживании
                            match slot_trackers.get_mut(&slot_num) {
                                Some(tracker) => {
                                    // Обновляем статус существующего слота
                                    tracker.update_status(status);
                                    
                                    // Если статус Finalized (2) - выводим сводку и удаляем
                                    if status as i32 == 2 {
                                        tracker.print_summary(slot_num);
                                        slot_trackers.remove(&slot_num);
                                    }
                                }
                                None => {
                                    // Создаем новый трекер для слота
                                    let creator = format!("slot_update_{:?}", status);
                                    let tracker = SlotTracker::new(creator, status);
                                    
                                    // Если сразу Finalized (2) - выводим и не сохраняем
                                    if status as i32 == 2 {
                                        tracker.print_summary(slot_num);
                                    } else {
                                        slot_trackers.insert(slot_num, tracker);
                                    }
                                }
                            }
                        }
                    }
                    Some(subscribe_update::UpdateOneof::Transaction(transaction)) => {
                        if let Some(_tx_info) = &transaction.transaction {
                            let slot_num = transaction.slot;
                            
                            // Обновляем трекер слота или создаем новый
                            match slot_trackers.get_mut(&slot_num) {
                                Some(tracker) => {
                                    // Обновляем существующий трекер
                                    tracker.update_transaction();
                                }
                                None => {
                                    // Создаем новый трекер для слота, инициированного транзакцией
                                    let creator = "transaction".to_string();
                                    let tracker = SlotTracker::new_from_transaction(creator);
                                    slot_trackers.insert(slot_num, tracker);
                                }
                            }
                            
                            // Выводим краткую информацию о транзакции (опционально)
                            // let signature = bs58::encode(&tx_info.signature).into_string();
                            // println!("TX: {} slot:{}", signature, slot_num);
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
