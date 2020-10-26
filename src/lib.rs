use std::sync::mpsc;
use std::thread;

use chrono::{DateTime, Utc};

use tungstenite::{connect, Message};

use url::Url;

use serde_json::{from_str, Value};

use std::fmt;

// 売買種別
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
    exec_date: DateTime<Utc>,
    exec_unix_time: i64,
    side: Side,
    price: f64,
    size: f64,
    channel: String,
}

impl Execution {
    // csvに書き込む用のデータを文字列として取得
    pub fn get_csv(&self) -> String {
        format!(
            "{} {} {} {}\n",
            self.exec_unix_time, self.side, self.price, self.size
        )
    }

    // 約定データの日付(年月日)を取得
    pub fn get_date(&self) -> String {
        self.exec_date.format("%Y%m%d").to_string()
    }

    // 約定データの日時(年月日時分秒)を取得
    pub fn get_date_second(&self) -> String {
        self.exec_date.format("%Y%m%d%H%M%S").to_string()
    }

    // 約定データのチャンネルを取得
    pub fn get_channel(&self) -> String {
        self.channel.to_string()
    }
}

// ストリーミングAPIのチャンネル
pub enum Channels {
    // 約定データのチャンネル
    Executions(Execution),
}

// ストリーミングAPIのデータを取得・送信する構造体
pub struct BfWebsocket {
    tx: mpsc::Sender<Channels>,
    rx: mpsc::Receiver<Channels>,
}

impl BfWebsocket {
    // ストリーミングAPIを処理するためのチャンネルを生成する
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel();
        BfWebsocket { tx, rx }
    }

    // ストリーミングAPIのエンドポイント
    pub fn get_end_point(&self) -> String {
        String::from("wss://ws.lightstream.bitflyer.com/json-rpc")
    }

    // ストリーミングAPIを利用して購読するチャンネル
    pub fn get_public_channels(&self) -> [String; 2] {
        [
            String::from("lightning_executions_FX_BTC_JPY"),
            String::from("lightning_executions_BTC_JPY"),
        ]
    }

    // ストリーミングAPIを利用して、チャンネルの購読を開始し、受信したメッセージを配信する
    pub fn on_connect(&self) {
        // 接続
        let (mut socket, _) =
            connect(Url::parse(&self.get_end_point()).unwrap()).expect("Can't connect");

        // チャンネルの購読を開始
        for public_channel in self.get_public_channels().iter() {
            let json = format!("{{\"jsonrpc\":\"2.0\",\"method\":\"subscribe\",\"params\":{{\"channel\":\"{}\"}}}}", public_channel);
            socket.write_message(Message::Text(json)).unwrap();
        }

        let tx = mpsc::Sender::clone(&self.tx);
        // 別スレッドを立ち上げて、メインスレッドに、受信したデータを配信する
        thread::spawn(move || loop {
            if let Ok(message) = socket.read_message() {
                match message {
                    Message::Text(text) => {
                        let v: Value = from_str(&text).unwrap();

                        for i in 0..v["params"]["message"].as_array().unwrap().len() {
                            let exec_date = v["params"]["message"][i]["exec_date"]
                                .as_str()
                                .unwrap()
                                .parse::<DateTime<Utc>>()
                                .unwrap();
                            let execute = Execution {
                                exec_date: exec_date,
                                exec_unix_time: exec_date.timestamp_nanos(),
                                side: Side::from_str(
                                    v["params"]["message"][i]["side"].as_str().unwrap(),
                                ),
                                price: v["params"]["message"][i]["price"].as_f64().unwrap(),
                                size: v["params"]["message"][i]["size"].as_f64().unwrap(),
                                channel: v["params"]["channel"].as_str().unwrap().to_string(),
                            };
                            tx.send(Channels::Executions(execute)).unwrap();
                        }
                    }
                    Message::Ping(data) => {
                        let pong = Message::Pong(data.clone());
                        socket.write_message(pong).unwrap();
                    }
                    _ => break,
                }
            }
        });
    }

    // 別スレッドからのメッセージを受け取る
    pub fn on_message(&self) -> Result<Channels, mpsc::RecvError> {
        self.rx.recv()
    }
}

