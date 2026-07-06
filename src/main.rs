use anyhow::{Result, anyhow};
use async_compression::tokio::write::{Lz4Decoder, Lz4Encoder};
use blake3::{Hash, Hasher};
use iroh::{
    Endpoint,
    endpoint::{RecvStream, presets},
};
use iroh_tickets::{Ticket, endpoint::EndpointTicket};
use std::{env, fs};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

const ALPN: &[u8; 10] = b"bifrost/v0";

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = env::args().skip(1);
    let file_or_ticket = args
        .next()
        .ok_or_else(|| anyhow!("[bifrost] Expected at least one argument."))?;

    let ticket = EndpointTicket::decode_string(&file_or_ticket).ok();

    if let Some(ticket) = ticket {
        return receive(ticket).await;
    }

    let file = fs::read(file_or_ticket)?;

    send(file).await
}

async fn send(file: Vec<u8>) -> Result<()> {
    let hash = blake3_hash(&file);

    let endpoint = Endpoint::builder(presets::N0)
        .alpns(vec![ALPN.to_vec()])
        .bind()
        .await?;

    endpoint.online().await;

    let ticket = EndpointTicket::new(endpoint.addr());

    println!("[bifrost] Use the following ticket to connect {}", ticket);

    let incoming = endpoint
        .accept()
        .await
        .ok_or_else(|| anyhow!("[bifrost] Incoming connection failed."))?;

    let connection = incoming.await?;

    let (mut send, mut recv) = connection.accept_bi().await?;
    println!("[bifrost] Connection established!");

    let _request = recv_bytes(&mut recv, 5).await?;

    send.write_all(hash.as_bytes()).await?;
    send.write_all(&file.len().to_be_bytes()).await?;

    let mut encoder = Lz4Encoder::new(&mut send);

    encoder.write_all(&file).await?;
    encoder.flush().await?;

    println!("[bifrost] Sent {} bytes", file.len());

    send.finish()?;

    let _response = recv_bytes(&mut recv, 2).await?;

    connection.close(0u32.into(), b"OK");

    endpoint.close().await;

    Ok(())
}

async fn receive(ticket: EndpointTicket) -> Result<()> {
    let endpoint = Endpoint::bind(presets::N0).await?;

    let connection = endpoint
        .connect(ticket.endpoint_addr().clone(), ALPN)
        .await?;

    let (mut send, mut recv) = connection.open_bi().await?;

    println!("[bifrost] Connection established!");

    send.write_all(b"START").await?;

    let received_hash = recv_bytes(&mut recv, 32).await?;

    let size_bytes = recv_bytes(&mut recv, std::mem::size_of::<usize>()).await?;
    let size = usize::from_be_bytes(size_bytes.try_into().unwrap());

    let mut response_buf = Vec::with_capacity(size);

    let mut decoder = Lz4Decoder::new(&mut response_buf);

    let _bytes_received = tokio::io::copy(&mut recv, &mut decoder).await?;
    decoder.flush().await?;

    println!("[bifrost] Received {} bytes", response_buf.len());

    let computed_hash = blake3_hash(&response_buf);

    let hash_verified = received_hash == computed_hash.as_bytes();

    if !hash_verified {
        return Err(anyhow!("[bifrost] Invalid file received."));
    }

    send.write_all(b"OK").await?;
    send.finish()?;

    connection.closed().await;

    let mut file = File::create("out.bin").await?;

    file.write_all(&response_buf).await?;

    Ok(())
}

async fn recv_bytes(stream: &mut RecvStream, length: usize) -> Result<Vec<u8>> {
    let mut buffer = vec![0u8; length];
    stream.read_exact(&mut buffer).await?;
    Ok(buffer)
}

fn blake3_hash(file: &Vec<u8>) -> Hash {
    let mut hasher = Hasher::new();
    hasher.update(file);
    hasher.finalize()
}
