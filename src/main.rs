extern crate fetch_market_and_order_data;

use std::fs::{create_dir_all, OpenOptions};
use std::io::{BufWriter, Write};

use fetch_market_and_order_data::ohlcv::{CandleStick, Periods};
use fetch_market_and_order_data::stream_api::{BfWebsocket, Common, Execution, MarketInfo};

use std::path::PathBuf;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "fetch_market_and_order_data")]
struct Opt {
    #[structopt(short, long, default_value("."))]
    output_dir: PathBuf,
}

fn main() {
    // コマンドライン引数から配信データ保存先を取得
    let opt = Opt::from_args();
    let output_dir = &opt.output_dir.display().to_string();

    // BitFlyerのストリーミングAPIに接続する
    let bf = BfWebsocket::new();
    bf.on_connect();

    // 1秒、5秒、15秒、1分、5分のローソク足を作成する
    let mut candle_sticks = vec![];
    for period in Periods::iterator() {
        candle_sticks.push(CandleStick::new(*period));
    }

    let exchange_name = bf.get_exchange_name();

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
                // 書き込み先は[{指定ディレクトリ}/{取引所}/{約定データの日付}/{約定データのチャンネル}.csv]
                let dir_all_name =
                    format!("{}/{}/{}", output_dir, exchange_name, execution.get_date());
                let file_name = execution.get_channel();
                append_csv(&dir_all_name, &file_name, execution.get_csv().as_bytes());

                // OHCLVデータを更新とCSVに書き込む
                // 書き込み先は[{指定ディレクトリ}/{取引所}/{約定データの日付}/{時間足}_{約定データのチャンネル}.csv]
                for i in 0..candle_sticks.len() {
                    update_ohlcv_and_append_csv(
                        &output_dir,
                        &exchange_name,
                        &mut candle_sticks[i],
                        &execution,
                    );
                }
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
            // 板データを受信した場合
            MarketInfo::Boards(board) => {
                // CSVに遅延時間を書き込む
                // 書き込み先は[{指定ディレクトリ}/{取引所}/{板データの日付}/{板データのチャンネル}.csv]
                let dir_all_name = format!("{}/{}/{}", output_dir, exchange_name, board.get_date());
                let file_name = format!("board_{}", board.get_channel());
                append_csv(&dir_all_name, &file_name, board.get_csv().as_bytes());
            }
        }
    }
}

// CSVファイルに追記モードで書き込む
fn append_csv(dir_all_name: &String, append_file_name: &String, content: &[u8]) {
    if let Err(error) = create_dir_all(format!("{}", dir_all_name)) {
        eprintln!("{}", error);
    }

    let file_name = format!("{}/{}.csv", dir_all_name, append_file_name);
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(file_name)
        .unwrap();
    let mut f = BufWriter::new(file);

    f.write(content).unwrap();
}

// OHCLVデータを更新とCSVに書き込む
fn update_ohlcv_and_append_csv(
    output_dir: &String,
    exchange_name: &String,
    candle_stick: &mut CandleStick,
    execution: &Execution,
) {
    if candle_stick.in_periods(&execution) {
        candle_stick.update(&execution);
        return;
    }
    if candle_stick.has_data() {
        let dir_all_name = format!("{}/{}/{}", output_dir, exchange_name, execution.get_date());
        let file_name = format!(
            "{}_{}",
            candle_stick.get_csv_name(),
            execution.get_channel(),
        );
        append_csv(&dir_all_name, &file_name, candle_stick.get_csv().as_bytes());
    }
    candle_stick.start(&execution);
}
