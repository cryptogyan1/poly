//! debug_tokens.rs — Find correct Polymarket token IDs
//!
//! Run with: cargo run --bin debug_tokens
//! 
//! Prints raw JSON from both Gamma API and CLOB API so we can
//! find exactly where the real 65-digit token IDs live.

use anyhow::Result;
use reqwest::Client;
use serde_json::Value;
use std::time::Duration;

const GAMMA_URL: &str = "https://gamma-api.polymarket.com";
const CLOB_URL:  &str = "https://clob.polymarket.com";

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let http = Client::builder().timeout(Duration::from_secs(10)).build()?;

    // ── Find current 15m BTC market ────────────────────────────────────────
    let now  = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?.as_secs();
    let base = (now / 900) * 900;

    let mut found_slug = None;
    for i in 0..=4u64 {
        let ts   = base - i * 900;
        let slug = format!("btc-updown-15m-{}", ts);
        let url  = format!("{}/events/slug/{}", GAMMA_URL, slug);

        println!("Trying: {}", url);
        if let Ok(resp) = http.get(&url).send().await {
            if resp.status().is_success() {
                let body = resp.text().await?;
                let json: Value = serde_json::from_str(&body)?;

                // Check if event has markets
                if json["markets"].as_array().map(|a| !a.is_empty()).unwrap_or(false) {
                    found_slug = Some((slug, body));
                    break;
                }
            }
        }
    }

    let (slug, raw_body) = match found_slug {
        Some(v) => v,
        None => { println!("❌ No BTC 15m market found"); return Ok(()); }
    };

    println!("\n✅ Found market: {}", slug);
    println!("\n════════════════════════════════════════");
    println!("RAW GAMMA API RESPONSE (first 3000 chars):");
    println!("════════════════════════════════════════");
    println!("{}", &raw_body[..raw_body.len().min(3000)]);

    // ── Parse and show all fields that contain token IDs ──────────────────
    println!("\n════════════════════════════════════════");
    println!("TOKEN ID FIELD ANALYSIS:");
    println!("════════════════════════════════════════");

    let json: Value = serde_json::from_str(&raw_body)?;

    // Top-level fields
    println!("\n[Top-level event fields]");
    if let Some(obj) = json.as_object() {
        for (key, val) in obj {
            if key.contains("token") || key.contains("Token") || key.contains("clob") || key.contains("condition") {
                println!("  event.{} = {}", key, val);
            }
        }
    }

    // Markets array
    if let Some(markets) = json["markets"].as_array() {
        println!("\n[Markets count: {}]", markets.len());
        for (i, market) in markets.iter().enumerate() {
            println!("\n  Market[{}]:", i);
            if let Some(obj) = market.as_object() {
                for (key, val) in obj {
                    // Show all fields - don't filter
                    let val_str = val.to_string();
                    if val_str.len() > 100 {
                        println!("    .{} = {}…", key, &val_str[..100]);
                    } else {
                        println!("    .{} = {}", key, val_str);
                    }
                }
            }
        }
    }

    // ── Now try CLOB API directly ──────────────────────────────────────────
    // Extract condition ID from the event
    let condition_id = json["markets"]
        .as_array()
        .and_then(|m| m.first())
        .and_then(|m| m["conditionId"].as_str())
        .unwrap_or("");

    if !condition_id.is_empty() {
        println!("\n════════════════════════════════════════");
        println!("CLOB API: /markets/{}", condition_id);
        println!("════════════════════════════════════════");

        let clob_url = format!("{}/markets/{}", CLOB_URL, condition_id);
        match http.get(&clob_url).send().await {
            Ok(resp) if resp.status().is_success() => {
                let body = resp.text().await?;
                let json: Value = serde_json::from_str(&body)?;

                println!("Raw CLOB response (first 2000 chars):");
                println!("{}", &body[..body.len().min(2000)]);

                println!("\n[CLOB Token fields]");
                if let Some(tokens) = json["tokens"].as_array() {
                    for (i, token) in tokens.iter().enumerate() {
                        println!("  tokens[{}].token_id = {}", i, token["token_id"]);
                        println!("  tokens[{}].outcome  = {}", i, token["outcome"]);
                    }
                }
            }
            Ok(resp) => println!("CLOB API HTTP {}", resp.status()),
            Err(e)   => println!("CLOB API error: {}", e),
        }
    }

    // ── Test WS with CLOB token IDs ───────────────────────────────────────
    println!("\n════════════════════════════════════════");
    println!("WEBSOCKET TEST with CLOB token IDs:");
    println!("════════════════════════════════════════");

    // Get all 4 token IDs (btc + sol)
    let mut all_tokens: Vec<String> = Vec::new();

    for prefix in &["btc", "sol"] {
        for i in 0..=4u64 {
            let ts   = base - i * 900;
            let slug = format!("{}-updown-15m-{}", prefix, ts);
            let url  = format!("{}/events/slug/{}", GAMMA_URL, slug);

            if let Ok(resp) = http.get(&url).send().await {
                if resp.status().is_success() {
                    if let Ok(body) = resp.text().await {
                        if let Ok(json) = serde_json::from_str::<Value>(&body) {
                            if let Some(markets) = json["markets"].as_array() {
                                for market in markets {
                                    let cid = market["conditionId"].as_str().unwrap_or("");
                                    if cid.is_empty() { continue; }

                                    // Try CLOB API for this market
                                    let clob_url = format!("{}/markets/{}", CLOB_URL, cid);
                                    if let Ok(cr) = http.get(&clob_url).send().await {
                                        if cr.status().is_success() {
                                            if let Ok(cbody) = cr.text().await {
                                                if let Ok(cjson) = serde_json::from_str::<Value>(&cbody) {
                                                    if let Some(tokens) = cjson["tokens"].as_array() {
                                                        for t in tokens {
                                                            if let Some(tid) = t["token_id"].as_str() {
                                                                println!("✅ {} | {} | token_id={}", prefix.to_uppercase(), t["outcome"].as_str().unwrap_or("?"), &tid[..tid.len().min(20)]);
                                                                all_tokens.push(tid.to_string());
                                                            }
                                                        }
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                if !all_tokens.is_empty() { break; }
            }
        }
    }

    if all_tokens.len() >= 4 {
        println!("\n✅ Got {} token IDs from CLOB API", all_tokens.len());
        println!("Full token IDs (length = {} chars each):", all_tokens[0].len());
        for t in &all_tokens {
            println!("  {}", t);
        }

        // Quick WS test with these IDs
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::connect_async;
        use tokio_tungstenite::tungstenite::Message;
        use tokio::time::timeout;

        let ws_url = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
        let (ws, _) = connect_async(ws_url).await?;
        let (mut write, mut read) = ws.split();

        let sub = serde_json::json!({
            "assets_ids": &all_tokens,
            "type": "market"
        });
        write.send(Message::Text(sub.to_string())).await?;
        println!("\nWS subscribed. Waiting 10s for messages…");

        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let mut msg_count = 0;

        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() { break; }

            if let Ok(Some(Ok(Message::Text(txt)))) = timeout(remaining, read.next()).await {
                let v: Value = serde_json::from_str(&txt).unwrap_or(Value::Null);
                let event = v["event_type"].as_str().unwrap_or(
                    v["type"].as_str().unwrap_or("unknown")
                );
                let asset = v["asset_id"].as_str().unwrap_or("").chars().take(16).collect::<String>();
                println!("  📨 msg#{} event_type={} asset={}…", msg_count+1, event, asset);
                msg_count += 1;
                if msg_count >= 5 { break; }
            }
        }

        if msg_count > 0 {
            println!("\n✅ CONFIRMED: CLOB token IDs work! Got {} WS messages.", msg_count);
            println!("🔧 FIX NEEDED: Update discover_tokens() in ws_diagnostic.rs to use CLOB API.");
            println!("   Also check main.rs / monitor — it may be passing wrong token IDs.");
        } else {
            println!("\n⚠️  No WS messages even with CLOB token IDs.");
            println!("   Markets may be between 15m periods. Try again in 1 minute.");
        }
    } else {
        println!("❌ Could not get 4 token IDs from CLOB API");
    }

    Ok(())
}
