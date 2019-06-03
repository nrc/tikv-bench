#![feature(async_await, await_macro, test, type_ascription)]

use tikv_client::{raw::Client, Config, Error, Value};
use std::time::Instant;

const PD: &str = "127.0.0.1:2379";
const COUNT: usize = 10_000;

async fn run_tests(pd: String) -> Result<(), Error> {
    let config = Config::new(&[pd]);
    let unconnnected_client = Client::new(config);
    let client: Client = unconnnected_client.await?;

    init(&client).await?;
    let start = Instant::now();
    present(&client).await?;
    not_present(&client).await?;
    let time = start.elapsed();
    println!("{}", time.as_millis());

    Ok(())
}

async fn init(client: &Client) -> Result<(), Error> {
    for i in 0..COUNT {
        let k: u64 = i as u64 * 8003;
        client.put((&k.to_le_bytes() as &[u8]).into(): Vec<u8>, make_value(i + 1)).await.unwrap();
    }
    Ok(())
}

#[inline]
fn make_value(len: usize) -> Vec<u8> {
    let mut result = Vec::with_capacity(len);
    for i in 0..len {
        result.push((i % 254) as u8);
    }
    result
}

async fn present(client: &Client) -> Result<(), Error> {
    for i in 0..COUNT {
        let k: u64 = i as u64 * 8003;
        let value: Option<Value> = client.get((&k.to_le_bytes() as &[u8]).into(): Vec<u8>).await?;
        assert_eq!(value.unwrap().len(), i + 1);
    }
    Ok(())
}

async fn not_present(client: &Client) -> Result<(), Error> {
    for i in 0..COUNT {
        let k: u64 = i as u64 * 8003 + 1;
        let value: Option<Value> = client.get((&k.to_le_bytes() as &[u8]).into(): Vec<u8>).await?;
        assert!(value.is_none());
    }
    Ok(())
}

#[runtime::main]
async fn main() {
    run_tests(PD.to_owned()).await.unwrap();
}
