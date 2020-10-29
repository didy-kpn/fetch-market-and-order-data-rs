use std::sync::mpsc;
use std::thread;

use chrono::{DateTime, Datelike, Utc};

use tungstenite::{connect, Message};

use url::Url;

use serde_json::{from_str, Value};

use std::fmt;

// 共通処理
pub trait Common {
    // csvに書き込む用のデータを文字列として取得
    fn get_csv(&self) -> String;

    fn data_time(&self) -> DateTime<Utc>;
    fn channel(&self) -> String;

    // データの日付(年月日)を取得
    fn get_date(&self) -> String {
        self.data_time().format("%Y%m%d").to_string()
    }

    // データの日時(年月日時分秒)を取得
    fn get_date_second(&self) -> String {
        self.data_time().format("%Y%m%d%H%M%S").to_string()
    }

    // データのチャンネルを取得
    fn get_channel(&self) -> String {
        self.channel().to_string()
    }
}

// 売買種別
#[derive(Clone, Copy)]
pub enum Side {
    Buy,
    Sell,
    NoSide,
}

// 売買種別のディスプレイ
impl fmt::Display for Side {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Side::Buy => write!(f, "B"),
            Side::Sell => write!(f, "S"),
            Side::NoSide => write!(f, "N"),
        }
    }
}

impl Side {
    // 文字列から売買種別の列挙型に変換する
    fn from_str(s: &str) -> Self {
        if s == "BUY" {
            Side::Buy
        } else if s == "SELL" {
            Side::Sell
        } else {
            Side::NoSide
        }
    }
}

// 約定履歴の構造体
pub struct Execution {
    id: i64,
    exec_date: DateTime<Utc>,
    exec_unix_time: i64,
    side: Side,
    price: f64,
    size: f64,
    channel: String,
}

impl Common for Execution {
    fn get_csv(&self) -> String {
        format!(
            "{} {} {} {} {}\n",
            self.id, self.exec_unix_time, self.side, self.price, self.size
        )
    }

    fn data_time(&self) -> DateTime<Utc> {
        self.exec_date
    }

    fn channel(&self) -> String {
        self.channel.clone()
    }
}

impl Execution {
    pub fn get_side(&self) -> Side {
        self.side
    }
    pub fn get_price(&self) -> f64 {
        self.price
    }
    pub fn get_size(&self) -> f64 {
        self.size
    }
}

// 遅延情報の構造体
pub struct Latency {
    sender_time: i64,
    receive_time: DateTime<Utc>,
    channel: String,
}

impl Common for Latency {
    fn get_csv(&self) -> String {
        format!(
            "{} {}\n",
            self.receive_time.timestamp_nanos(),
            self.sender_time
        )
    }

    fn data_time(&self) -> DateTime<Utc> {
        self.receive_time
    }

    fn channel(&self) -> String {
        self.channel.clone()
    }
}

// 板情報の構造体
pub struct Board {
    receive_time: DateTime<Utc>,
    json: String,
    channel: String,
}

impl Common for Board {
    fn get_csv(&self) -> String {
        format!("{} {}\n", self.receive_time.timestamp_nanos(), self.json)
    }

    fn data_time(&self) -> DateTime<Utc> {
        self.receive_time
    }

    fn channel(&self) -> String {
        self.channel.clone()
    }
}

// ストリーミングAPIから得られる取引所からのマーケット情報
pub enum MarketInfo {
    // 約定データ
    Executions(Execution),

    // 遅延データ
    LatencyExchange(Latency),

    // 板情報データ
    Boards(Board),
}

// ストリーミングAPIのデータを取得・送信する構造体
pub struct BfWebsocket {
    exchange_name: String,
    tx: mpsc::Sender<MarketInfo>,
    rx: mpsc::Receiver<MarketInfo>,
}

impl BfWebsocket {
    pub fn get_exchange_name(&self) -> String {
        self.exchange_name.clone()
    }

    // ストリーミングAPIを処理するためのチャンネルを生成する
    pub fn new() -> Self {
        let exchange_name = String::from("bitFlyer");
        let (tx, rx) = mpsc::channel();
        BfWebsocket {
            exchange_name,
            tx,
            rx,
        }
    }

    // ストリーミングAPIのエンドポイント
    pub fn get_end_point(&self) -> String {
        String::from("wss://ws.lightstream.bitflyer.com/json-rpc")
    }

    // ストリーミングAPIを利用して購読するチャンネル
    pub fn get_public_channels(&self) -> [String; 6] {
        [
            String::from("lightning_executions_FX_BTC_JPY"),
            String::from("lightning_executions_BTC_JPY"),
            String::from("lightning_board_snapshot_FX_BTC_JPY"),
            String::from("lightning_board_FX_BTC_JPY"),
            String::from("lightning_board_snapshot_BTC_JPY"),
            String::from("lightning_board_BTC_JPY"),
        ]
    }

    // ストリーミングAPIの約定履歴チャンネル
    pub fn get_public_execute_channels(&self) -> [String; 2] {
        [
            String::from("lightning_executions_FX_BTC_JPY"),
            String::from("lightning_executions_BTC_JPY"),
        ]
    }

    // ストリーミングAPIの板情報チャンネル
    pub fn get_public_board_channels(&self) -> [String; 2] {
        [
            String::from("lightning_board_FX_BTC_JPY"),
            String::from("lightning_board_BTC_JPY"),
        ]
    }

    // ストリーミングAPIのスナップショットチャンネル
    pub fn get_public_snapshot_channels(&self) -> [String; 2] {
        [
            String::from("lightning_board_snapshot_FX_BTC_JPY"),
            String::from("lightning_board_snapshot_BTC_JPY"),
        ]
    }

    // ストリーミングAPIを利用して、チャンネルの購読を開始し、受信したメッセージを配信する
    pub fn on_connect(&self) {
        // 接続
        let (mut socket, _) =
            connect(Url::parse(&self.get_end_point()).unwrap()).expect("Can't connect");

        // チャンネルの購読を開始
        for public_channel in self.get_public_channels().iter() {
            let json = format!(
        "{{\"jsonrpc\":\"2.0\",\"method\":\"subscribe\",\"params\":{{\"channel\":\"{}\"}}}}",
        public_channel
      );
            socket.write_message(Message::Text(json)).unwrap();
        }

        let tx = mpsc::Sender::clone(&self.tx);
        let public_execute_channels = self.get_public_execute_channels().clone();
        let public_board_channels = self.get_public_board_channels().clone();
        let public_snapshot_channels = self.get_public_snapshot_channels().clone();

        // 別スレッドを立ち上げて、メインスレッドに、受信したデータを配信する
        thread::spawn(move || {
            // 前回の接続した日付
            let mut last_connected_date = Utc::today();
            loop {
                // 現在の日付を取得し、前回と日が異なる場合はスナップショットチャンネルに再接続する
                let connect_time = Utc::today();
                if last_connected_date.day() != connect_time.day() {
                    for snapshot_channel in public_snapshot_channels.iter() {
                        let json = format!(
                    "{{\"jsonrpc\":\"2.0\",\"method\":\"subscribe\",\"params\":{{\"channel\":\"{}\"}}}}",
                    snapshot_channel
                  );
                        socket.write_message(Message::Text(json)).unwrap();
                        last_connected_date = connect_time;
                    }
                }

                if let Ok(message) = socket.read_message() {
                    match message {
                        Message::Text(text) => {
                            // 受信時間
                            let receive_time = Utc::now();

                            // 約定履歴データの一番古い日時を代入する用
                            let mut exec_ts_nanos = 5_000_000_000_000_000_000;

                            let v: Value = from_str(&text).unwrap();

                            let channel = v["params"]["channel"].as_str().unwrap().to_string();

                            // 受信データが約定履歴の場合、
                            if 0 < public_execute_channels
                                .iter()
                                .filter(|&x| x == &channel)
                                .count()
                            {
                                // 約定データを配信する
                                for i in 0..v["params"]["message"].as_array().unwrap().len() {
                                    let exec_date = v["params"]["message"][i]["exec_date"]
                                        .as_str()
                                        .unwrap()
                                        .parse::<DateTime<Utc>>()
                                        .unwrap();
                                    let exec_unix_time = exec_date.timestamp_nanos();
                                    let execute = Execution {
                                        id: v["params"]["message"][i]["id"].as_i64().unwrap(),
                                        exec_date: exec_date,
                                        exec_unix_time: exec_unix_time,
                                        side: Side::from_str(
                                            v["params"]["message"][i]["side"].as_str().unwrap(),
                                        ),
                                        price: v["params"]["message"][i]["price"].as_f64().unwrap(),
                                        size: v["params"]["message"][i]["size"].as_f64().unwrap(),
                                        channel: channel.clone(),
                                    };
                                    tx.send(MarketInfo::Executions(execute)).unwrap();

                                    exec_ts_nanos = std::cmp::min(exec_ts_nanos, exec_unix_time);
                                }

                                // 遅延データを配信する
                                let latency = Latency {
                                    sender_time: receive_time.timestamp_nanos() - exec_ts_nanos,
                                    receive_time: receive_time,
                                    channel: channel.clone(),
                                };
                                tx.send(MarketInfo::LatencyExchange(latency)).unwrap();

                            // 受信データが板情報の差分の場合、
                            } else if 0 < public_board_channels
                                .iter()
                                .filter(|&x| x == &channel)
                                .count()
                            {
                                // 板データの差分を配信する
                                let board = Board {
                                    receive_time: receive_time,
                                    json: v["params"]["message"].to_string(),
                                    channel: channel.clone(),
                                };
                                tx.send(MarketInfo::Boards(board)).unwrap();

                            // 受信データがスナップショットの場合、
                            } else if 0 < public_snapshot_channels
                                .iter()
                                .filter(|&x| x == &channel)
                                .count()
                            {
                                // スナップショットの購読を停止する
                                let json = format!(
                "{{\"jsonrpc\":\"2.0\",\"method\":\"unsubscribe\",\"params\":{{\"channel\":\"{}\"}}}}",
                channel
              );
                                socket.write_message(Message::Text(json)).unwrap();

                                // 板データのスナップショットを配信する
                                let board = Board {
                                    receive_time: receive_time,
                                    json: v["params"]["message"].to_string(),
                                    channel: channel.replace("_snapshot", ""),
                                };
                                tx.send(MarketInfo::Boards(board)).unwrap();
                            } else {
                                continue;
                            }
                        }
                        Message::Ping(data) => {
                            let pong = Message::Pong(data.clone());
                            socket.write_message(pong).unwrap();
                        }
                        _ => break,
                    }
                }
            }
        });
    }

    // 別スレッドからのメッセージを受け取る
    pub fn on_message(&self) -> Result<MarketInfo, mpsc::RecvError> {
        self.rx.recv()
    }
}
