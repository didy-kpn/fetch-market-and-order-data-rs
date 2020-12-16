extern crate fetch_market_and_order_data;

use std::fs::{create_dir_all, OpenOptions};
use std::io::{BufWriter, Write};
use fetch_market_and_order_data::stream_api::{BfWebsocket, Common, MarketInfo};

use std::path::PathBuf;
use structopt::StructOpt;

use std::thread::sleep;
use std::time::{Duration, SystemTime};

use std::sync::mpsc::TryRecvError;

use env_logger;
use log::{info, warn, error};
use std::env;

#[derive(StructOpt, Debug)]
#[structopt(name = "fetch_market_and_order_data")]
struct Opt {
    #[structopt(short, long, default_value("."))]
    output_dir: PathBuf,
}

fn main() {
    env::set_var("RUST_LOG", "info");
    env_logger::init();

    // コマンドライン引数から配信データ保存先を取得
    let opt = Opt::from_args();
    let output_dir = &opt.output_dir.display().to_string();

    loop {
        // BitFlyerのストリーミングAPIに接続する
        let bf = BfWebsocket::new();
        bf.on_connect();
        info!("Connect to bitFlyer Websocket Service.");

        let exchange_name = bf.get_exchange_name();

        let mut last_recv_time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("back to the future")
            .as_secs();

        // ストリーミングAPIから配信される情報を取得する
        loop {
            let now_recv_time = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("back to the future")
                .as_secs();
            let bf_on_message = bf.on_message();
            if let Err(error) = bf_on_message {
                match error {
                    // 空データを3分以上受信した場合は再接続する
                    TryRecvError::Empty => {
                        if 180 <= now_recv_time - last_recv_time {
                            warn!("bf_on_message: Empty data received for more than 3 minutes.");
                            break;
                        }
                        sleep(Duration::from_millis(1));
                    }
                    // 切断エラーの場合は再接続をする
                    TryRecvError::Disconnected => {
                        warn!("bf_on_message: Disconnected.");
                        break;
                    }
                }
            }

            if let Ok(message) = bf_on_message {
                last_recv_time = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .expect("back to the future")
                    .as_secs();
                match message {
                    // 約定データを受信した場合
                    MarketInfo::Executions(execution) => {
                        // CSVに約定データを書き込む
                        // 書き込み先は[{指定ディレクトリ}/{取引所}/{約定データの日付}/{約定データのチャンネル}.csv]
                        let dir_all_name =
                            format!("{}/{}/{}", output_dir, exchange_name, execution.get_date());
                        append_csv(&dir_all_name, &execution.get_channel(), execution.get_csv().as_bytes());
                    }
                    // 遅延データを受信した場合
                    MarketInfo::LatencyExchange(latency) => {
                        // CSVに遅延時間を書き込む
                        // 書き込み先は[{指定ディレクトリ}/{取引所}/{遅延データの日付}/{遅延データのチャンネル}.csv]
                        let dir_all_name =
                            format!("{}/{}/{}", output_dir, exchange_name, latency.get_date());
                        let file_name = format!("latency_{}", latency.get_channel());
                        append_csv(&dir_all_name, &file_name, latency.get_csv().as_bytes());
                    }
                    // 受信終了の場合
                    MarketInfo::Close => {
                        info!("Received Close Message.");
                        break;
                    }
                    _ => {}
                }
            }
        }

        // ストリーミングAPIからの配信を停止する
        bf.close_thread();
        sleep(Duration::from_secs(10));
        info!("Disconnect to bitFlyer Websocket Service.");
    }
}

// CSVファイルに追記モードで書き込む
fn append_csv(dir_all_name: &String, append_file_name: &String, content: &[u8]) {
    if let Err(error) = create_dir_all(format!("{}", dir_all_name)) {
        error!("append_csv.create_dir_all: {}", error);
        return;
    }

    let file_name = format!("{}/{}.csv", dir_all_name, append_file_name);
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(file_name)
        .unwrap();
    let mut f = BufWriter::new(file);

    if let Err(error) = f.write(content) {
        error!("append_csv.f.write(content): {}", error);
    }
}
