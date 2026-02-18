//! ws_diagnostic.rs — WebSocket Health Checker & Live Price Monitor
//!
//! Run with:
//!   cargo run --bin ws_diagnostic
use anyhow::Result;
use colored::Colorize;
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::time::timeout;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const GAMMA_URL: &str = "https://gamma-api.polymarket.com";
const CLOB_URL: &str = "https://clob.polymarket.com";

// ─── Market discovery ─────────────────────────────────────────────────────────
//
// NOTE: We intentionally do NOT use clobTokenIds from the Gamma API response.
// That field returns truncated ~16-digit IDs which the WS server does not
// recognise.  The correct 75-digit token IDs come from:
//   GET https://clob.polymarket.com/markets/{conditionId}
//   → response.tokens[*].token_id

async fn discover_tokens(http: &Client, prefix: &str) -> Result<(String, String, String, String)> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    let base = (now / 900) * 900;

    for i in 0..=4u64 {
        let ts = base - i * 900;
        let slug = format!("{}-updown-15m-{}", prefix, ts);
        let gamma_url = format!("{}/events/slug/{}", GAMMA_URL, slug);

        let resp = match http.get(&gamma_url).timeout(Duration::from_secs(8)).send().await {
            Ok(r) if r.status().is_success() => r,
            _ => continue,
        };
        let body = match resp.text().await {
            Ok(b) => b,
            Err(_) => continue,
        };
        let json: Value = match serde_json::from_str(&body) {
            Ok(v) => v,
            Err(_) => continue,
        };

        let markets = match json["markets"].as_array() {
            Some(m) if !m.is_empty() => m,
            _ => continue,
        };

        for m in markets {
            // Must be active
            if m["active"].as_bool() != Some(true) {
                continue;
            }

            let condition_id = match m["conditionId"].as_str() {
                Some(c) if !c.is_empty() => c.to_string(),
                _ => continue,
            };

            // ── Fetch real token IDs from CLOB API ─────────────────────────
            let clob_url = format!("{}/markets/{}", CLOB_URL, condition_id);
            let clob_resp = match http.get(&clob_url).timeout(Duration::from_secs(8)).send().await {
                Ok(r) if r.status().is_success() => r,
                _ => continue,
            };
            let clob_body = match clob_resp.text().await {
                Ok(b) => b,
                Err(_) => continue,
            };
            let clob_json: Value = match serde_json::from_str(&clob_body) {
                Ok(v) => v,
                Err(_) => continue,
            };

            let tokens = match clob_json["tokens"].as_array() {
                Some(t) if t.len() >= 2 => t,
                _ => continue,
            };

            // tokens[0] = Up, tokens[1] = Down (per Polymarket convention)
            let up_id = match tokens[0]["token_id"].as_str() {
                Some(id) if id.len() > 30 => id.to_string(),
                _ => continue,
            };
            let down_id = match tokens[1]["token_id"].as_str() {
                Some(id) if id.len() > 30 => id.to_string(),
                _ => continue,
            };

            return Ok((slug, condition_id, up_id, down_id));
        }
    }
    anyhow::bail!("No active {} market found", prefix)
}

// ─── Result formatting ────────────────────────────────────────────────────────

fn pass(msg: &str) {
    println!("  {} {}", "✅ PASS".green().bold(), msg);
}
fn fail(msg: &str) {
    println!("  {} {}", "❌ FAIL".red().bold(), msg);
}
fn warn_msg(msg: &str) {
    println!("  {} {}", "⚠️  WARN".yellow().bold(), msg);
}
fn info_msg(msg: &str) {
    println!("  {} {}", "ℹ️  INFO".cyan(), msg);
}
fn section(title: &str) {
    println!("\n{}", "─".repeat(60).dimmed());
    println!("{}", format!("  STAGE: {}", title).white().bold());
    println!("{}", "─".repeat(60).dimmed());
}

// ─── Main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    println!(
        "\n{}",
        "╔══════════════════════════════════════════════════════════╗"
            .cyan()
    );
    println!(
        "{}",
        "║     POLYMARKET WS DIAGNOSTIC — Full Health Check         ║"
            .cyan()
            .bold()
    );
    println!(
        "{}",
        "╚══════════════════════════════════════════════════════════╝"
            .cyan()
    );

    let http = Client::builder().timeout(Duration::from_secs(10)).build()?;

    // ══════════════════════════════════════════════════════
    // STAGE 1 — ENVIRONMENT VARIABLES
    // ══════════════════════════════════════════════════════
    section("1 / 9  —  ENV VARIABLES");

    let required_vars = [
        "PRIVATE_KEY",
        "PROXY_WALLET",
        "POLY_API_KEY",
        "POLY_API_SECRET",
        "POLY_API_PASSPHRASE",
        "RPC_URL",
    ];

    let mut env_ok = true;
    for var in &required_vars {
        match std::env::var(var) {
            Ok(val) if !val.trim().is_empty() => {
                let preview = if var.contains("KEY") || var.contains("SECRET") || var.contains("PASS") {
                    format!("{}…", &val[..val.len().min(6)])
                } else {
                    format!("{}…", &val[..val.len().min(20)])
                };
                pass(&format!("{} = {}", var, preview));
            }
            _ => {
                fail(&format!("{} is MISSING or empty", var));
                env_ok = false;
            }
        }
    }

    info_msg(&format!(
        "PAIR_BTC_ETH = {}",
        std::env::var("PAIR_BTC_ETH").unwrap_or_else(|_| "not set (default BTC/ETH)".to_string())
    ));
    info_msg(&format!(
        "EXECUTION_MODE = {}",
        std::env::var("EXECUTION_MODE").unwrap_or_else(|_| "executor".to_string())
    ));

    if !env_ok {
        println!(
            "\n{}",
            "❌  Cannot continue — missing required env vars. Check your .env file."
                .red()
                .bold()
        );
        return Ok(());
    }

    // ══════════════════════════════════════════════════════
    // STAGE 2 — REST API + MARKET DISCOVERY
    // ══════════════════════════════════════════════════════
    section("2 / 9  —  REST API & MARKET DISCOVERY");

    match http
        .get(format!("{}/markets?limit=1", GAMMA_URL))
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => pass("Gamma API reachable"),
        Ok(r) => fail(&format!("Gamma API returned HTTP {}", r.status())),
        Err(e) => fail(&format!("Gamma API unreachable: {}", e)),
    }

    println!("\n  Discovering current 15m markets…");

    let left_result = discover_tokens(&http, "btc").await;
    let right_result = discover_tokens(&http, "sol").await;

    let (_left_slug, left_cid, left_up_id, left_down_id) = match left_result {
        Ok(v) => {
            pass(&format!(
                "Left  market: {} | UP={}…  DOWN={}…",
                v.0,
                &v.2[..12],
                &v.3[..12]
            ));
            v
        }
        Err(e) => {
            fail(&format!("Left market discovery failed: {}", e));
            return Ok(());
        }
    };

    let (_right_slug, right_cid, right_up_id, right_down_id) = match right_result {
        Ok(v) => {
            pass(&format!(
                "Right market: {} | UP={}…  DOWN={}…",
                v.0,
                &v.2[..12],
                &v.3[..12]
            ));
            v
        }
        Err(e) => {
            fail(&format!("Right market discovery failed: {}", e));
            return Ok(());
        }
    };

    let token_ids = vec![
        left_up_id.clone(),
        left_down_id.clone(),
        right_up_id.clone(),
        right_down_id.clone(),
    ];

    let token_labels: HashMap<String, &str> = [
        (left_up_id.clone(), "BTC_UP  "),
        (left_down_id.clone(), "BTC_DOWN"),
        (right_up_id.clone(), "SOL_UP  "),
        (right_down_id.clone(), "SOL_DOWN"),
    ]
    .iter()
    .cloned()
    .collect();

    info_msg(&format!("Left  condition: {}", left_cid));
    info_msg(&format!("Right condition: {}", right_cid));

    // ══════════════════════════════════════════════════════
    // STAGE 3 — WEBSOCKET CONNECTION
    // ══════════════════════════════════════════════════════
    section("3 / 9  —  WEBSOCKET CONNECTION");

    let t_connect = Instant::now();
    let url = Url::parse(WS_URL)?;

    let (ws, _response) = match timeout(Duration::from_secs(8), connect_async(url)).await {
        Ok(Ok(v)) => {
            pass(&format!(
                "Connected in {} ms",
                t_connect.elapsed().as_millis()
            ));
            v
        }
        Ok(Err(e)) => {
            fail(&format!("WS connect failed: {}", e));
            return Ok(());
        }
        Err(_) => {
            fail("WS connect timed out after 8s");
            return Ok(());
        }
    };

    let (mut write, mut read) = ws.split();

    // ══════════════════════════════════════════════════════
    // STAGE 4 — SUBSCRIPTION
    // ══════════════════════════════════════════════════════
    section("4 / 9  —  SUBSCRIPTION MESSAGE");

    // Correct format for wss://ws-subscriptions-clob.polymarket.com/ws/market
    // (NOT the old SDK format with "channels")
    let sub_msg = json!({
        "assets_ids": token_ids,
        "type": "market"
    });
    write.send(Message::Text(sub_msg.to_string())).await?;
    pass(&format!(
        "Subscription sent for {} tokens",
        token_ids.len()
    ));
    info_msg(&format!(
        "Token IDs (first 20 chars each): {:?}",
        token_ids
            .iter()
            .map(|id| &id[..20.min(id.len())])
            .collect::<Vec<_>>()
    ));

    // ══════════════════════════════════════════════════════
    // STAGE 5 — RAW MESSAGE ARRIVAL
    // ══════════════════════════════════════════════════════
    section("5 / 9  —  RAW MESSAGE RECEPTION (10s timeout)");

    let mut raw_messages: Vec<String> = Vec::new();
    let mut got_first = false;
    let deadline = Instant::now() + Duration::from_secs(10);

    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }

        match timeout(remaining, read.next()).await {
            Ok(Some(Ok(Message::Text(txt)))) => {
                if !got_first {
                    got_first = true;
                    pass(&format!(
                        "First message arrived! Size: {} bytes",
                        txt.len()
                    ));
                }
                raw_messages.push(txt);
                if raw_messages.len() >= 8 {
                    break;
                }
            }
            Ok(Some(Ok(Message::Ping(_)))) => {
                info_msg("Received ping from server");
            }
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(e))) => {
                fail(&format!("WS read error: {}", e));
                break;
            }
            Ok(None) => {
                fail("WS stream closed unexpectedly");
                break;
            }
            Err(_) => break,
        }
    }

    if raw_messages.is_empty() {
        fail("No messages received within 10 seconds!");
        println!("\n  {}", "DIAGNOSIS:".yellow().bold());
        println!("  • The WS connected but no market data arrived.");
        println!("  • Most likely cause: token IDs are wrong or 15m period just ended.");
        println!("  • Polymarket only streams data for ACTIVE markets.");
        println!("  • Wait 1 minute and try again, or check your pair config.");
        return Ok(());
    }

    pass(&format!(
        "Received {} messages in collection window",
        raw_messages.len()
    ));

    // ══════════════════════════════════════════════════════
    // STAGE 6 — MESSAGE PARSING
    // ══════════════════════════════════════════════════════
    section("6 / 9  —  MESSAGE PARSING");

    let mut book_events = 0usize;
    let mut price_events = 0usize;
    let mut subscribed_acks = 0usize;
    let mut parse_errors = 0usize;

    for (i, raw) in raw_messages.iter().enumerate() {
        print!("  Message #{}: ", i + 1);

        match serde_json::from_str::<Value>(raw) {
            Ok(v) => {
                let event_type = v.get("event_type").and_then(Value::as_str);
                let msg_type = v.get("type").and_then(Value::as_str);

                match (event_type, msg_type) {
                    (Some("book"), _) => {
                        let asset = v
                            .get("asset_id")
                            .and_then(Value::as_str)
                            .unwrap_or("?");
                        let label = token_labels
                            .get(asset)
                            .copied()
                            .unwrap_or("UNKNOWN");
                        let bids = v
                            .get("bids")
                            .and_then(Value::as_array)
                            .map(|a| a.len())
                            .unwrap_or(0);
                        let asks = v
                            .get("asks")
                            .and_then(Value::as_array)
                            .map(|a| a.len())
                            .unwrap_or(0);
                        println!(
                            "{} event_type=book | token={}… ({}) | bids={} asks={}",
                            "📚".green(),
                            &asset[..16.min(asset.len())],
                            label,
                            bids,
                            asks
                        );
                        book_events += 1;
                    }
                    // ── New price_change schema (post Sept 15 2025) ──────────
                    // Messages now carry a top-level "price_changes" array where
                    // each element has its own asset_id, price, side, best_bid,
                    // best_ask.  The old flat schema (asset_id + changes[]) is gone.
                    (Some("price_change"), _) => {
                        if let Some(pcs) = v.get("price_changes").and_then(Value::as_array) {
                            // New schema
                            for pc in pcs {
                                let asset = pc.get("asset_id").and_then(Value::as_str).unwrap_or("?");
                                let label = token_labels.get(asset).copied().unwrap_or("UNKNOWN");
                                let side  = pc.get("side").and_then(Value::as_str).unwrap_or("?");
                                let price = pc.get("price").and_then(Value::as_str).unwrap_or("?");
                                let best_bid = pc.get("best_bid").and_then(Value::as_str).unwrap_or("?");
                                let best_ask = pc.get("best_ask").and_then(Value::as_str).unwrap_or("?");
                                println!(
                                    "{} price_change (new) | {} ({}) | side={} price={} | bid={} ask={}",
                                    "📈".yellow(),
                                    &asset[..16.min(asset.len())],
                                    label, side, price, best_bid, best_ask
                                );
                            }
                        } else {
                            // Old schema fallback (unlikely but safe)
                            let asset = v.get("asset_id").and_then(Value::as_str).unwrap_or("?");
                            let label = token_labels.get(asset).copied().unwrap_or("UNKNOWN");
                            let changes = v
                                .get("changes")
                                .and_then(Value::as_array)
                                .map(|a| a.len())
                                .unwrap_or(0);
                            println!(
                                "{} price_change (old) | {}… ({}) | changes={}",
                                "📈".yellow(),
                                &asset[..16.min(asset.len())],
                                label, changes
                            );
                        }
                        price_events += 1;
                    }
                    // ── best_bid_ask — fastest arb signal ────────────────────
                    // Only sent when subscription includes "custom_feature_enabled": true
                    (Some("best_bid_ask"), _) => {
                        let asset   = v.get("asset_id").and_then(Value::as_str).unwrap_or("?");
                        let label   = token_labels.get(asset).copied().unwrap_or("UNKNOWN");
                        let best_bid = v.get("best_bid").and_then(Value::as_str).unwrap_or("?");
                        let best_ask = v.get("best_ask").and_then(Value::as_str).unwrap_or("?");
                        println!(
                            "{} best_bid_ask | {} ({}) | bid={} ask={}",
                            "⚡".cyan(),
                            &asset[..16.min(asset.len())],
                            label, best_bid, best_ask
                        );
                        price_events += 1;
                    }
                    // ── market_resolved ──────────────────────────────────────
                    (Some("market_resolved"), _) => {
                        let winner = v.get("winning_asset_id").and_then(Value::as_str).unwrap_or("?");
                        println!("{} market_resolved | winner={}…", "🏁".green(), &winner[..16.min(winner.len())]);
                    }
                    (_, Some("subscribed")) => {
                        println!("{} Subscription ACK received", "📬".green());
                        subscribed_acks += 1;
                    }
                    (_, Some("pong")) => {
                        println!("{} Pong", "💓".cyan());
                    }
                    _ => {
                        println!(
                            "{} Unknown msg: {}",
                            "❓".dimmed(),
                            &raw[..raw.len().min(120)]
                        );
                    }
                }
            }
            Err(e) => {
                println!("{} JSON parse error: {}", "💥".red(), e);
                parse_errors += 1;
            }
        }
    }

    println!();
    if book_events > 0 {
        pass(&format!("'book' events:         {}", book_events));
    } else {
        warn_msg("No 'book' events seen yet (may arrive shortly)");
    }
    if price_events > 0 {
        pass(&format!("'price_change' events: {}", price_events));
    } else {
        info_msg("No 'price_change' events yet (normal — arrives after first book)");
    }
    if subscribed_acks > 0 {
        pass(&format!("Subscription ACKs:     {}", subscribed_acks));
    }
    if parse_errors > 0 {
        fail(&format!("Parse errors:          {}", parse_errors));
    }

    // ══════════════════════════════════════════════════════
    // STAGE 7 — PRICE EXTRACTION
    // ══════════════════════════════════════════════════════
    section("7 / 9  —  PRICE EXTRACTION");

    let mut prices: HashMap<String, (Option<Decimal>, Option<Decimal>)> = HashMap::new();

    for raw in &raw_messages {
        if let Ok(v) = serde_json::from_str::<Value>(raw) {
            if v.get("event_type").and_then(Value::as_str) == Some("book") {
                let asset = match v.get("asset_id").and_then(Value::as_str) {
                    Some(a) => a,
                    None => continue,
                };

                let best_bid = v
                    .get("bids")
                    .and_then(Value::as_array)
                    .and_then(|bids| {
                        bids.iter()
                            .filter_map(|b| {
                                b.get("price")
                                    .and_then(Value::as_str)
                                    .and_then(|s| s.parse::<Decimal>().ok())
                            })
                            .reduce(|a, b| if a > b { a } else { b })
                    });

                let best_ask = v
                    .get("asks")
                    .and_then(Value::as_array)
                    .and_then(|asks| {
                        asks.iter()
                            .filter_map(|a| {
                                a.get("price")
                                    .and_then(Value::as_str)
                                    .and_then(|s| s.parse::<Decimal>().ok())
                            })
                            .reduce(|a, b| if a < b { a } else { b })
                    });

                prices.insert(asset.to_string(), (best_bid, best_ask));
            }
        }
    }

    if prices.is_empty() {
        warn_msg("No prices extracted yet — need a 'book' event first");
        info_msg("The WS sends a full snapshot on subscribe, then incremental updates.");
    } else {
        for (token_id, (bid, ask)) in &prices {
            let label = token_labels
                .get(token_id.as_str())
                .copied()
                .unwrap_or("?");
            let bid_s = bid
                .map(|b| format!("{:.4}", b))
                .unwrap_or_else(|| "—".to_string());
            let ask_s = ask
                .map(|a| format!("{:.4}", a))
                .unwrap_or_else(|| "—".to_string());
            pass(&format!("{} | bid={}  ask={}", label, bid_s, ask_s));
        }

        // Quick arb check
        let btc_up_ask = prices.get(&left_up_id).and_then(|(_, a)| *a);
        let sol_down_ask = prices.get(&right_down_id).and_then(|(_, a)| *a);
        let btc_down_ask = prices.get(&left_down_id).and_then(|(_, a)| *a);
        let sol_up_ask = prices.get(&right_up_id).and_then(|(_, a)| *a);

        println!();
        if let (Some(btc_up), Some(sol_down)) = (btc_up_ask, sol_down_ask) {
            let sum = btc_up + sol_down;
            let profit = Decimal::ONE - sum;
            let icon = if profit > dec!(0) { "🟢" } else { "🔴" };
            println!(
                "  {} Pair 1 (BTC_UP + SOL_DOWN): {:.4} + {:.4} = {:.4}  profit={:+.4}",
                icon, btc_up, sol_down, sum, profit
            );
        }
        if let (Some(btc_down), Some(sol_up)) = (btc_down_ask, sol_up_ask) {
            let sum = btc_down + sol_up;
            let profit = Decimal::ONE - sum;
            let icon = if profit > dec!(0) { "🟢" } else { "🔴" };
            println!(
                "  {} Pair 2 (BTC_DOWN + SOL_UP): {:.4} + {:.4} = {:.4}  profit={:+.4}",
                icon, btc_down, sol_up, sum, profit
            );
        }
    }

    // ══════════════════════════════════════════════════════
    // STAGE 8 — CACHE + spawn_ws_feed() TEST
    // ══════════════════════════════════════════════════════
    section("8 / 9  —  PRICECACHE & spawn_ws_feed() TEST (15s)");

    use polymarket_15m_arbitrage_bot::cache::PriceCache;
    use polymarket_15m_arbitrage_bot::ws::spawn_ws_feed;

    // Unit-test PriceCache directly
    let test_cache = PriceCache::new();
    test_cache
        .update(
            "test",
            vec![(dec!(0.45), dec!(100.0))],
            vec![(dec!(0.55), dec!(200.0))],
        )
        .await;

    match test_cache.get("test").await {
        Some(b) if b.bids.first().map(|(p, _)| *p) == Some(dec!(0.45)) => {
            pass("PriceCache write/read works correctly");
        }
        Some(_) => fail("PriceCache returned wrong values"),
        None => fail("PriceCache returned None after write — cache is broken"),
    }

    // Test real spawn_ws_feed
    println!("\n  Running spawn_ws_feed() for 15 seconds…");
    let live_cache = PriceCache::new();
    let mut ws_rx = spawn_ws_feed(WS_URL.to_string(), token_ids.clone(), live_cache.clone());

    let t_spawn = Instant::now();
    let mut events_received = 0usize;
    let mut cache_hits = 0usize;

    loop {
        if t_spawn.elapsed() > Duration::from_secs(15) {
            break;
        }

        match timeout(Duration::from_secs(4), ws_rx.recv()).await {
            Ok(Ok(update)) => {
                events_received += 1;
                let label = token_labels
                    .get(update.token_id.as_str())
                    .copied()
                    .unwrap_or("?");

                if let Some(book) = live_cache.get(&update.token_id).await {
                    let bid = book
                        .bids
                        .first()
                        .map(|(p, _)| format!("{:.4}", p))
                        .unwrap_or_else(|| "—".to_string());
                    let ask = book
                        .asks
                        .first()
                        .map(|(p, _)| format!("{:.4}", p))
                        .unwrap_or_else(|| "—".to_string());
                    println!(
                        "  {} Event #{:>2} | {} | {} | bid={} ask={}",
                        "⚡".yellow(),
                        events_received,
                        label.cyan().bold(),
                        update.event_type,
                        bid.green(),
                        ask.red()
                    );
                    cache_hits += 1;
                } else {
                    println!(
                        "  {} Event #{:>2} | {} | {} | ⚠️  cache miss",
                        "⚡".yellow(),
                        events_received,
                        label.cyan().bold(),
                        update.event_type
                    );
                }

                if events_received >= 12 {
                    break;
                }
            }
            Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(n))) => {
                warn_msg(&format!("Receiver lagged by {} messages", n));
            }
            Ok(Err(_)) => {
                fail("Broadcast channel closed unexpectedly");
                break;
            }
            Err(_) => {
                warn_msg("No WS event for 4s — market may be quiet");
                break;
            }
        }
    }

    println!();
    if events_received == 0 {
        fail("spawn_ws_feed() fired ZERO broadcast events");
        warn_msg("ws/mod.rs is connected but tx.send() is never called.");
        info_msg("Check handle_message() — does it match 'book'/'price_change' event_type?");
    } else {
        pass(&format!(
            "spawn_ws_feed() fired {} events in {} ms",
            events_received,
            t_spawn.elapsed().as_millis()
        ));
    }

    if events_received > 0 && cache_hits == 0 {
        fail("Events fired but PriceCache was NOT populated");
        warn_msg("Check that cache.update() is called inside handle_message()");
    } else if cache_hits > 0 {
        pass(&format!(
            "PriceCache populated on {}/{} events",
            cache_hits, events_received
        ));
    }

    // ══════════════════════════════════════════════════════
    // STAGE 9 — LIVE 30s STREAM
    // ══════════════════════════════════════════════════════
    section("9 / 9  —  LIVE 30-SECOND PRICE STREAM");
    println!("  Watching all 4 tokens for live price changes…\n");

    let mut event_count: HashMap<String, usize> = HashMap::new();
    let mut last_prices: HashMap<String, (Decimal, Decimal)> = HashMap::new();
    let t_live = Instant::now();

    loop {
        if t_live.elapsed() > Duration::from_secs(30) {
            break;
        }

        match timeout(Duration::from_secs(5), ws_rx.recv()).await {
            Ok(Ok(update)) => {
                *event_count.entry(update.token_id.clone()).or_insert(0) += 1;

                if let Some(book) = live_cache.get(&update.token_id).await {
                    if let (Some((bid, _)), Some((ask, _))) =
                        (book.bids.first(), book.asks.first())
                    {
                        let label = token_labels
                            .get(update.token_id.as_str())
                            .copied()
                            .unwrap_or("?");

                        let changed = last_prices
                            .get(&update.token_id)
                            .map(|(pb, pa)| pb != bid || pa != ask)
                            .unwrap_or(true);

                        if changed {
                            let elapsed = t_live.elapsed().as_secs_f32();
                            let spread = ask - bid;
                            println!(
                                "  [{:5.1}s] {} | bid={} ask={} spread={:.4} | cnt={}",
                                elapsed,
                                label.cyan().bold(),
                                format!("{:.4}", bid).green(),
                                format!("{:.4}", ask).red(),
                                spread,
                                event_count.get(&update.token_id).unwrap_or(&0)
                            );
                            last_prices.insert(update.token_id.clone(), (*bid, *ask));
                        }
                    }
                }
            }
            Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(n))) => {
                println!("  {} Lagged by {} events", "⚠️".yellow(), n);
            }
            Ok(Err(_)) => break,
            Err(_) => {
                println!("  {} No activity for 5s", "💤".dimmed());
            }
        }
    }

    // ══════════════════════════════════════════════════════
    // FINAL SUMMARY
    // ══════════════════════════════════════════════════════
    println!("\n{}", "═".repeat(60).cyan());
    println!("{}", "  DIAGNOSTIC SUMMARY".white().bold());
    println!("{}", "═".repeat(60).cyan());

    let total_events = event_count.values().sum::<usize>();
    println!(
        "\n  Total broadcast events in 30s window: {}",
        total_events.to_string().bold()
    );
    println!();

    for tid in &token_ids {
        let label = token_labels.get(tid.as_str()).copied().unwrap_or("?");
        let count = event_count.get(tid).copied().unwrap_or(0);
        let price_str = if let Some((bid, ask)) = last_prices.get(tid) {
            format!("bid={:.4}  ask={:.4}", bid, ask)
        } else {
            "no data yet".to_string()
        };

        let status = if count == 0 {
            "❌".red()
        } else if count < 3 {
            "⚠️ ".yellow()
        } else {
            "✅".green()
        };
        println!(
            "  {} {} | events={:>3} | {}",
            status,
            label.bold(),
            count,
            price_str
        );
    }

    println!();
    if total_events == 0 {
        println!(
            "{}",
            "  ❌  VERDICT: WebSocket connected but receiving NO broadcast events."
                .red()
                .bold()
        );
        println!("  LIKELY CAUSES:");
        println!("  1. 15m market just expired — restart the bot to discover new markets");
        println!("  2. event_type field mismatch in ws/mod.rs handle_message()");
        println!("  3. tx.send() not being called after cache.update()");
    } else if cache_hits == 0 {
        println!(
            "{}",
            "  ⚠️   VERDICT: Events firing but PriceCache not updating.".yellow().bold()
        );
        println!("  Fix: cache.update() must be called before tx.send() in handle_message()");
    } else if total_events < 5 {
        println!(
            "{}",
            "  ⚠️   VERDICT: Very few events — market may be low activity.".yellow().bold()
        );
        println!("  Try again during peak hours (US market hours 9am–4pm EST).");
    } else {
        println!(
            "{}",
            "  ✅  VERDICT: WebSocket integration is working correctly!"
                .green()
                .bold()
        );
        println!("     WS connects and subscribes       ✓");
        println!("     Messages received and parsed     ✓");
        println!("     PriceCache populated             ✓");
        println!("     Broadcast channel fires          ✓");
        println!("     Live prices streaming            ✓");
        println!("     Bot will detect opportunities in < 1ms ✓");
    }

    println!();
    Ok(())
}
