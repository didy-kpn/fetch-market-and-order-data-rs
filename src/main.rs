extern crate fetch_market_and_order_data;

use std::fs::OpenOptions;
use std::io::{BufWriter, Write};

use fetch_market_and_order_data::{BfWebsocket, Channels};

fn main() {
    // BitFlyerのリアルタイムAPIに接続する
    let bf = BfWebsocket::new();
    bf.on_connect();

    loop {
        // リアルタイムAPIから配信される情報を取得する
        let message = bf.on_message();

        if let Err(err) = message {
            eprintln!("{}", err);
            continue;
        }

        match message.unwrap() {
            // 約定データを受信した場合
            Channels::Executions(text) => {

                // CSVに日時、売買種別、価格、注文量を書き込む
                // 書き込み先は[bitflyer_{約定データのチャンネル}_{約定データの日付}.csv]
                let file_name = format!("bitflyer_{}_{}.csv", text.get_channel(), text.get_date());
                let file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(file_name)
                    .unwrap();
                let mut f = BufWriter::new(file);

                f.write(text.get_csv().as_bytes()).unwrap();
            }
        }
    }
}
