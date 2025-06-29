use archipelago_rs::client::ArchipelagoClient;
use std::io::{self, BufRead};
use std::str::FromStr;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    // Connect to AP server
    let server = prompt("Connect to what AP server?")?;
    let (host, port) = server.split_once(':').unwrap();

    let mut client = ArchipelagoClient::new(&host, u16::from_str(port).unwrap()).await?;
    println!("Connected!");

    // Connect to a given slot on the server

    let game = prompt("What game?")?;
    let slot = prompt("What slot?")?;
    client
        .connect(&game, &slot, None, Some(7), vec!["AP".to_string()])
        .await?;
    println!("Connected to slot!");

    client.say("Hello, world!").await?;
    println!("Sent Hello, world!");

    Ok(())
}

fn prompt(text: &str) -> Result<String, anyhow::Error> {
    println!("{}", text);

    Ok(io::stdin().lock().lines().next().unwrap()?)
}
