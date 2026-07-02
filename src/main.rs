use anyhow::{Result, anyhow};
use blake3::{Hash, Hasher};
use iroh::{
    Endpoint,
    endpoint::{Connection, RecvStream, presets},
    protocol::{AcceptError, ProtocolHandler, Router},
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
    let endpoint = Endpoint::bind(presets::N0).await?;

    endpoint.online().await;

    let ticket = EndpointTicket::new(endpoint.addr());

    println!("[bifrost] Use the following ticket to connect {}", ticket);

    let bifrost = Bifrost::new(file);

    let router = Router::builder(endpoint).accept(ALPN, bifrost).spawn();

    tokio::signal::ctrl_c().await?;

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

    let response = recv.read_to_end(size).await?;

    println!("[bifrost] Received {} bytes", response.len());

    let computed_hash = blake3_hash(&response);

    let hash_verified = received_hash == computed_hash.as_bytes();

    if !hash_verified {
        return Err(anyhow!("[bifrost] Invalid file received."));
    }

    send.write_all(b"OK").await?;
    send.finish()?;

    connection.closed().await;

    let mut file = File::create("out.bin").await?;

    file.write_all(&response).await?;

    Ok(())
}
#[derive(Debug)]
struct Bifrost {
    file: Vec<u8>,
    hash: Hash,
}

impl ProtocolHandler for Bifrost {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let (mut send, mut recv) = connection.accept_bi().await?;
        println!("[bifrost] Connection established!");

        let _request = recv_bytes(&mut recv, 5).await.unwrap();

        let file = &self.file;

        send.write_all(self.hash.as_bytes()).await.unwrap();
        send.write_all(&file.len().to_be_bytes()).await.unwrap();

        // TODO
        // Sender: Hash, compress, send
        // Receiver: Receive, decompress, hash
        send.write_all(&self.file).await.unwrap();

        println!("[bifrost] Sent {} bytes", file.len());

        send.finish()?;

        let _response = recv_bytes(&mut recv, 2).await.unwrap();

        connection.close(0u32.into(), b"OK");

        Ok(())
    }
}

impl Bifrost {
    pub fn new(file: Vec<u8>) -> Self {
        Self {
            hash: blake3_hash(&file),
            file,
        }
    }
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
