use crate::stream_api::{Board, Common};
use chrono::{DateTime, Duration, TimeZone, Utc};
use std::collections::{HashMap, HashSet};
use crate::periods::Periods;

// OHLCVの構造体
#[derive(Debug)]
pub struct OrderBooks {
    date: DateTime<Utc>,
    asks: HashMap<String, f64>,
    bids: HashMap<String, f64>,
    periods: Periods,
}

impl OrderBooks {
    pub fn new(periods_str: String) -> OrderBooks {
        let now_timestamp = Utc::now().timestamp();
        OrderBooks {
            date: Utc.timestamp(now_timestamp / 60 * 60, 0),
            asks: HashMap::new(),
            bids: HashMap::new(),
            periods: Periods::new(periods_str),
        }
    }

    // 単位期間のはじめ
    pub fn start(&mut self, board: &Board) {
        self.asks.clear();
        self.bids.clear();
        for i in 0..board.asks.len() {
            let count = self.asks.entry(board.asks[i].0.to_string()).or_insert(0f64);
            *count += board.asks[i].1;
        }

        for i in 0..board.bids.len() {
            let count = self.bids.entry(board.bids[i].0.to_string()).or_insert(0f64);
            *count += board.bids[i].1;
        }

        let now_timestamp = board.data_time().timestamp();
        let pd = self.periods.time();
        self.date = Utc.timestamp(now_timestamp / pd * pd, 0);
    }

    // 単位期間のデータを更新
    pub fn update(&mut self, board: &Board) {
        if !board.is_update {
            self.start(board);
            return;
        }
        let mut used_asks_key = HashSet::new();
        let mut used_bids_key = HashSet::new();
        for i in 0..board.asks.len() {
            if board.asks[i].0 == 0f64 {
                continue;
            }

            if board.asks[i].1 == 0f64 {
                self.asks.remove(&board.asks[i].0.to_string());
                continue;
            }
            if !used_asks_key.contains(&board.asks[i].0.to_string()) {
                self.asks.remove(&board.asks[i].0.to_string());
                used_asks_key.insert(board.asks[i].0.to_string());
            }

            let count = self.asks.entry(board.asks[i].0.to_string()).or_insert(0f64);
            *count += board.asks[i].1;
        }

        for i in 0..board.bids.len() {
            if board.bids[i].0 == 0f64 {
                continue;
            }

            if board.bids[i].1 == 0f64 {
                self.bids.remove(&board.bids[i].0.to_string());
                continue;
            }
            if !used_bids_key.contains(&board.bids[i].0.to_string()) {
                self.asks.remove(&board.bids[i].0.to_string());
                used_bids_key.insert(board.bids[i].0.to_string());
            }

            let count = self.bids.entry(board.bids[i].0.to_string()).or_insert(0f64);
            *count += board.bids[i].1;
        }

        let now_timestamp = board.data_time().timestamp();
        let pd = self.periods.time();
        self.date = Utc.timestamp(now_timestamp / pd * pd, 0);
    }

    // 単位期間のデータかどうか
    pub fn in_periods(&self, board: &Board) -> bool {
        let board_timestamp = board.data_time().timestamp();
        let pd = self.periods.time();
        let dt = Utc.timestamp(board_timestamp / pd * pd, 0);
        let duration: Duration = dt - self.date;
        duration.num_seconds() == 0
    }

    // 単位期間内にデータがあるかどうか
    pub fn has_data(&self) -> bool {
        !self.asks.is_empty() || !self.bids.is_empty()
    }

    // csvに書き込む用のデータを文字列として取得
    pub fn get_csv(&self) -> String {

        let mut asks = self.asks.iter().map(|(k, v)| (k.parse::<f64>().unwrap(), v)).collect::<Vec<(f64, &f64)>>();
        asks.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

        let mut bids = self.bids.iter().map(|(k, v)| (k.parse::<f64>().unwrap(), v)).collect::<Vec<(f64, &f64)>>();
        bids.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());

        format!(
            "{} {} {}\n",
            self.date.timestamp(),
            asks[..25]
                .iter()
                .map(|(k, v)| format!("[{},{}]", k, v))
                .collect::<Vec<String>>()
                .join(","),
            bids[..25]
                .iter()
                .map(|(k, v)| format!("[{},{}]", k, v))
                .collect::<Vec<String>>()
                .join(","),
        )
    }

    // csv名に利用する文字列を取得
    pub fn get_csv_name(&self) -> String {
        self.periods.to_str()
    }
}
