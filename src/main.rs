extern crate fetch_market_and_order_data;

use std::fs::OpenOptions;
use std::io::{BufWriter, Write};

use fetch_market_and_order_data::stream_api::{BfWebsocket, Common, MarketInfo};

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
                let file_name = format!("{}_{}", execution.get_channel(), execution.get_date());
                append_csv(file_name, execution.get_csv().as_bytes());
            }
            // 遅延データを受信した場合
            MarketInfo::LatencyExchange(latency) => {
                // CSVに遅延時間を書き込む
                // 書き込み先は[bitflyer_{遅延データのチャンネル}_{遅延データの日付}.csv]
                let file_name = format!("latency_{}_{}", latency.get_channel(), latency.get_date());
                append_csv(file_name, latency.get_csv().as_bytes());
            }
            // 板データを受信した場合
            MarketInfo::Boards(board) => {
                // CSVに遅延時間を書き込む
                // 書き込み先は[bitflyer_{板データのチャンネル}_{板データの日付}.csv]
                let file_name = format!("board_{}_{}", board.get_channel(), board.get_date());
                append_csv(file_name, board.get_csv().as_bytes());
            }
        }
    }
}

// CSVファイルに追記モードで書き込む
fn append_csv(append_file_name: String, content: &[u8]) {
    let file_name = format!("bitflyer_{}.csv", append_file_name);
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(file_name)
        .unwrap();
    let mut f = BufWriter::new(file);

    f.write(content).unwrap();
}
