use std::sync::{Arc, mpsc, atomic::{AtomicBool, Ordering} };
use std::thread;

use chrono::{DateTime, Timelike, Utc};

use tungstenite::{connect, Message};

use url::Url;

use serde_json::{from_str, Value};

use std::fmt;

use log::{info, warn, error};

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
#[derive(Clone)]
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
            self.receive_time.timestamp_millis(),
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
    pub asks: Vec<(f64, f64)>,
    pub bids: Vec<(f64, f64)>,
    channel: String,
    pub is_update: bool,
}

impl Common for Board {
    // NOTE:
    fn get_csv(&self) -> String {
        format!("{}\n", self.receive_time.timestamp_millis())
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

    // 受信終了
    Close,
}

// ストリーミングAPIのデータを取得・送信する構造体
pub struct BfWebsocket {
    exchange_name: String,
    tx: mpsc::Sender<MarketInfo>,
    rx: mpsc::Receiver<MarketInfo>,
    finish: Arc<AtomicBool>,
}

impl BfWebsocket {
    pub fn get_exchange_name(&self) -> String {
        self.exchange_name.clone()
    }

    // ストリーミングAPIを処理するためのチャンネルを生成する
    pub fn new() -> Self {
        let exchange_name = String::from("bitFlyer");
        let (tx, rx) = mpsc::channel();
        let finish = Arc::new(AtomicBool::new(false));
        BfWebsocket {
            exchange_name,
            tx,
            rx,
            finish,
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
            info!("on_connect: Subscribe {}", public_channel);
        }

        let tx = mpsc::Sender::clone(&self.tx);
        let public_channels = self.get_public_channels();
        let public_execute_channels = self.get_public_execute_channels().clone();
        let public_board_channels = self.get_public_board_channels().clone();
        let public_snapshot_channels = self.get_public_snapshot_channels().clone();
        let finish = self.finish.clone();

        // 別スレッドを立ち上げて、メインスレッドに、受信したデータを配信する
        thread::spawn(move || {

            // 前回の接続した日付
            let mut last_connected_date = Utc::now();
            loop {

                // チャンネルの購読を停止
                if (*finish).load(Ordering::Relaxed) {
                    for public_channel in public_channels.iter() {
                        let json = format!("{{\"jsonrpc\":\"2.0\",\"method\":\"unsubscribe\",\"params\":{{\"channel\":\"{}\"}}}}", public_channel);
                        if let Err(error) = socket.write_message(Message::Text(json)) {
                            error!("on_connect.thread: Unsubscribe {}. {}", public_channel, error);
                        } else {
                            info!("on_connect.thread: Unsubscribe {}", public_channel);
                        }
                    }
                    info!("on_connect.thread: thread Finish.");
                    break;
                }

                // 現在の日付を取得し、前回と日が異なる場合はスナップショットチャンネルに再接続する
                let connect_time = Utc::now();
                if last_connected_date.hour() != connect_time.hour() {
                    for snapshot_channel in public_snapshot_channels.iter() {
                        let json = format!(
                    "{{\"jsonrpc\":\"2.0\",\"method\":\"subscribe\",\"params\":{{\"channel\":\"{}\"}}}}",
                    snapshot_channel
                  );
                        if let Err(error) = socket.write_message(Message::Text(json)) {
                            error!("on_connect.thread: Subscribe {}. {}", snapshot_channel, error);
                            continue;
                        } else {
                            info!("on_connect.thread: Subscribe {}", snapshot_channel);
                        }
                        last_connected_date = connect_time;
                    }
                }

                // 接続等でエラーが発生した場合は終了する
                let socket_read_message = socket.read_message();
                if let Err(error) = socket_read_message {
                    (*finish).store(true, Ordering::Relaxed);
                    error!("on_connect.thread: True the exit flag. {}", error);
                    continue;
                }
                let socket_read_message = socket_read_message.unwrap();
                match socket_read_message {
                    Message::Text(text) => {
                        // 受信時間
                        let receive_time = Utc::now();

                        // 約定履歴データの一番古い日時を代入する用
                        let mut exec_ts_millis = 5_000_000_000_000;

                        let v: Value = from_str(&text).unwrap();

                        let channel = v["params"]["channel"].as_str().unwrap().to_string();

                        // 受信データが約定履歴の場合、
                        if 0 < public_execute_channels
                            .iter()
                            .filter(|&x| x == &channel)
                            .count()
                        {
                            let mut executes = Vec::new();
                            // 約定データを配信する
                            for i in 0..v["params"]["message"].as_array().unwrap().len() {
                                let exec_date = v["params"]["message"][i]["exec_date"]
                                    .as_str()
                                    .unwrap()
                                    .parse::<DateTime<Utc>>()
                                    .unwrap();
                                let exec_unix_time = exec_date.timestamp_millis();
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
                                exec_ts_millis = std::cmp::min(exec_ts_millis, exec_unix_time);
                                executes.push(execute);
                            }
                            executes.sort_by(|a, b| a.exec_unix_time.cmp(&b.exec_unix_time)); // 日付を古い順でソートする
                            for i in 0..executes.len() {
                                tx.send(MarketInfo::Executions(executes[i].clone())).unwrap();
                            }

                            // 遅延データを配信する
                            let latency = Latency {
                                sender_time: receive_time.timestamp_millis() - exec_ts_millis,
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
                            let mut asks = Vec::new();
                            let mut bids = Vec::new();
                            for i in 0..v["params"]["message"]["asks"].as_array().unwrap().len() {
                                let price = v["params"]["message"]["asks"][i]["price"].as_f64().unwrap();
                                let size = v["params"]["message"]["asks"][i]["size"].as_f64().unwrap();
                                let v = (price, size);
                                asks.push(v);
                            }

                            for i in 0..v["params"]["message"]["bids"].as_array().unwrap().len() {
                                let price = v["params"]["message"]["bids"][i]["price"].as_f64().unwrap();
                                let size = v["params"]["message"]["bids"][i]["size"].as_f64().unwrap();
                                let v = (price, size);
                                bids.push(v);
                            }
                            // 板データの差分を配信する
                            let board = Board {
                                receive_time: receive_time,
                                asks,
                                bids,
                                channel: channel.clone(),
                                is_update: true,
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
                            if let Err(error) = socket.write_message(Message::Text(json)) {
                                error!("on_connect.thread: Unsubscribe {}. {}", channel, error);
                            } else {
                                info!("on_connect.thread: Unsubscribe {}", channel);
                            }

                            let mut asks = Vec::new();
                            let mut bids = Vec::new();
                            for i in 0..v["params"]["message"]["asks"].as_array().unwrap().len() {
                                let price = v["params"]["message"]["asks"][i]["price"].as_f64().unwrap();
                                let size = v["params"]["message"]["asks"][i]["size"].as_f64().unwrap();
                                let v = (price, size);
                                asks.push(v);
                            }

                            for i in 0..v["params"]["message"]["bids"].as_array().unwrap().len() {
                                let price = v["params"]["message"]["bids"][i]["price"].as_f64().unwrap();
                                let size = v["params"]["message"]["bids"][i]["size"].as_f64().unwrap();
                                let v = (price, size);
                                bids.push(v);
                            }

                            // 板データのスナップショットを配信する
                            let board = Board {
                                receive_time: receive_time,
                                asks,
                                bids,
                                channel: channel.replace("_snapshot", ""),
                                is_update: false,
                            };
                            tx.send(MarketInfo::Boards(board)).unwrap();
                        } else {
                            continue;
                        }
                    }
                    Message::Ping(data) => {
                        let pong = Message::Pong(data.clone());
                        if let Err(error) = socket.write_message(pong) {
                            error!("on_connect.thread: Received Ping Message and try to send Pong Message. {}", error);
                        }
                    }
                    Message::Close(_) => {
                        (*finish).store(true, Ordering::Relaxed);
                        tx.send(MarketInfo::Close).unwrap();
                        warn!("on_connect.thread: True the exit flag. Received Close Message.");
                        continue;
                    }
                    _ => continue,
                }
            }
        });
    }

    // 別スレッドからのメッセージを受け取る
    pub fn on_message(&self) -> Result<MarketInfo, mpsc::TryRecvError> {
        self.rx.try_recv()
    }

    // メッセージ受信用のスレッドを停止する
    pub fn close_thread(&self) {
        let finish = self.finish.clone();
        (*finish).store(true, Ordering::Relaxed);
        warn!("close_thread: True the exit flag.");
    }
}
