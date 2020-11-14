use crate::stream_api::{Common, Execution, Side};
use chrono::{DateTime, Duration, TimeZone, Utc};

// ローソク足の期間(秒数、秒数の文字表記)
#[derive(Debug)]
pub struct Periods {
    periods: i64,         // 秒数
    periods_str: String,  // 秒数の文字表記。例: 1s, 2m, 3h, 4d
}

impl Periods {
    // 秒数の文字表記を指定し、ローソク足の期間(秒数)を保存する
    pub fn new(periods_str: String) -> Periods {
        let mut ps = periods_str.clone();

        // 後ろの文字を取得
        let cs = ps.pop();
        if cs.is_none() {
            panic!("periods_str is empty string.");
        }
        let unit = match cs.unwrap() {
            's' => 1,
            'm' => 60,
            'h' => 3600,
            'd' => 86400,
            _ => 0,
        };

        // 後ろ以外の文字を数値に変換
        let no = ps.parse::<i64>();
        if no.is_err() {
            panic!("not number.");
        }
        let no = no.unwrap();

        // 期間を秒に変換する
        let periods = no * unit;
        if periods == 0 {
            panic!("invalid value.");
        }

        Periods {
            periods,
            periods_str
        }
    }

    fn time(&self) -> i64 {
        self.periods
    }

    fn to_str(&self) -> String {
        self.periods_str.clone()
    }
}

// OHLCVの構造体
#[derive(Clone, Copy, Debug)]
struct OHLCV {
    date: DateTime<Utc>,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    buy_volume: f64,
    sell_volume: f64,
    volume: f64,
}

// ローソク足の構造体
#[derive(Debug)]
pub struct CandleStick {
    ohlcv: Option<OHLCV>,
    periods: Periods,
}

impl CandleStick {
    pub fn new(periods_str: String) -> CandleStick {
        CandleStick {
            ohlcv: None,
            periods: Periods::new(periods_str),
        }
    }

    // 単位期間のはじめ
    pub fn start(&mut self, execution: &Execution) {
        let exec_timestamp = execution.data_time().timestamp();
        let price = execution.get_price();
        let size = execution.get_size();
        let side = execution.get_side();
        let pd = self.periods.time();
        let ohlcv = OHLCV {
            date: Utc.timestamp(exec_timestamp / pd * pd, 0),

            open: price,
            high: price,
            low: price,
            close: price,
            buy_volume: match side {
                Side::Buy => size,
                _ => 0.0,
            },
            sell_volume: match side {
                Side::Sell => size,
                _ => 0.0,
            },

            volume: size,
        };
        self.ohlcv = Some(ohlcv);
    }

    // 単位期間のデータを更新
    pub fn update(&mut self, execution: &Execution) {
        if let Some(ohlcv) = self.ohlcv {
            let mut ohlcv = ohlcv;

            let price = execution.get_price();
            let size = execution.get_size();
            let side = execution.get_side();

            ohlcv.close = price;
            ohlcv.high = ohlcv.high.max(price);
            ohlcv.low = ohlcv.low.max(price);
            if let Side::Buy = side {
                ohlcv.buy_volume += size;
            }
            if let Side::Sell = side {
                ohlcv.sell_volume += size;
            }
            ohlcv.volume += size;

            self.ohlcv = Some(ohlcv);
        }
    }

    // 単位期間のデータかどうか
    pub fn in_periods(&self, execution: &Execution) -> bool {
        if self.ohlcv.is_none() {
            return false;
        }

        let pd = self.periods.time();
        let exec_timestamp = execution.data_time().timestamp();
        let dt = Utc.timestamp(exec_timestamp / pd * pd, 0);
        let duration: Duration = dt - self.ohlcv.unwrap().date;
        duration.num_seconds() == 0
    }

    // 単位期間内にデータがあるかどうか
    pub fn has_data(&self) -> bool {
        self.ohlcv.is_some()
    }

    // csvに書き込む用のデータを文字列として取得
    pub fn get_csv(&self) -> String {
        let ohlcv = self.ohlcv.unwrap();
        format!(
            "{} {} {} {} {} {} {} {}\n",
            ohlcv.date.timestamp_nanos(),
            ohlcv.open,
            ohlcv.high,
            ohlcv.low,
            ohlcv.close,
            ohlcv.buy_volume,
            ohlcv.sell_volume,
            ohlcv.volume,
        )
    }

    // csv名に利用する文字列を取得
    pub fn get_csv_name(&self) -> String {
        self.periods.to_str()
    }
}
