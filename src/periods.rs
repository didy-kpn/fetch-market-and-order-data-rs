// 期間(秒数、秒数の文字表記)
#[derive(Debug)]
pub struct Periods {
    periods: i64,         // 秒数
    periods_str: String,  // 秒数の文字表記。例: 1s, 2m, 3h, 4d
}

impl Periods {
    // 秒数の文字表記を指定し、期間(秒数)を保存する
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

    pub fn time(&self) -> i64 {
        self.periods
    }

    pub fn to_str(&self) -> String {
        self.periods_str.clone()
    }
}
