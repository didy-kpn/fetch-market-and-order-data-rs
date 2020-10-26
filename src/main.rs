extern crate fetch_market_and_order_data;

use std::fs::OpenOptions;
use std::io::{BufWriter, Write};

use fetch_market_and_order_data::{BfWebsocket, MarketInfo};

fn main() {
    // BitFlyerのストリーミングAPIに接続する
    let bf = BfWebsocket::new();
    bf.on_connect();

    loop {
        // ストリーミングAPIから配信される情報を取得する
        let message = bf.on_message();

        if let Err(err) = message {
            eprintln!("{}", err);
            continue;
        }

        match message.unwrap() {
            // 約定データを受信した場合
            MarketInfo::Executions(execution) => {
                // CSVに日時、売買種別、価格、注文量を書き込む
                // 書き込み先は[bitflyer_{約定データのチャンネル}_{約定データの日付}.csv]
                let file_name = format!("bitflyer_{}_{}.csv", execution.get_channel(), execution.get_date());
                let file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(file_name)
                    .unwrap();
                let mut f = BufWriter::new(file);

                f.write(execution.get_csv().as_bytes()).unwrap();
            }
            // 遅延データを受信した場合
            MarketInfo::LatencyExchange(latency) => {
                // CSVに遅延時間を書き込む
                // 書き込み先は[bitflyer_{遅延データのチャンネル}_{遅延データの日付}.csv]
                let file_name = format!("bitflyer_latency_{}_{}.csv", latency.get_channel(), latency.get_date());
                let file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(file_name)
                    .unwrap();
                let mut f = BufWriter::new(file);

                f.write(latency.get_csv().as_bytes()).unwrap();
            }
        }
    }
}
