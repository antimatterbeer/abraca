use std::{cmp::Reverse, collections::BTreeMap};

/// 双边订单薄。
///
/// - `asks`：卖方，升序存储，`first_key_value()` = 最优卖价（最低）。
/// - `bids`：买方，以 `Reverse<i64>` 为 key 实现降序，`first_key_value()` = 最优买价（最高）。
/// - `timestamp`：最后更新时间戳。
/// - `serial_no`：最后更新序列号。
pub struct OrderBook {
    asks: BTreeMap<i64, f64>,
    bids: BTreeMap<Reverse<i64>, f64>,
    decimal_places: u32, // 价格小数位数
    multiplier: f64,     // 价格缩放因子
}

impl OrderBook {
    /// 创建一个新订单薄。
    pub fn new(price_decimal_places: u32) -> Self {
        Self {
            asks: BTreeMap::new(),
            bids: BTreeMap::new(),
            decimal_places: price_decimal_places,
            multiplier: 10_f64.powi(price_decimal_places as i32),
        }
    }

    /// 清空订单薄。
    pub fn clear(&mut self) {
        self.asks.clear();
        self.bids.clear();
    }

    /// 更新订单薄
    ///
    /// # 参数
    /// - `timestamp`：最后更新时间戳。
    /// - `serial_no`：最后更新序列号。
    /// - `bids`：买方，以 `(f64, f64)` 表示，第一项为价格，第二项为数量。
    /// - `asks`：卖方，以 `(f64, f64)` 表示，第一项为价格，第二项为数量。
    pub fn update(&mut self, bids: &[(f64, f64)], asks: &[(f64, f64)]) {
        for (price, qty) in bids {
            let encoded_price = self.encode_price(*price);
            if *qty == 0.0 {
                self.bids.remove(&Reverse(encoded_price));
            } else {
                self.bids.insert(Reverse(encoded_price), *qty);
            }
        }

        for (price, qty) in asks {
            let encoded_price = self.encode_price(*price);
            if *qty == 0.0 {
                self.asks.remove(&encoded_price);
            } else {
                self.asks.insert(encoded_price, *qty);
            }
        }
    }

    /// 检查订单薄是否有效，即卖价大于买价。
    ///
    /// # 返回
    /// - `true`：订单薄有效。
    /// - `false`：订单薄无效。
    pub fn check_validity(&self) -> bool {
        match (self.asks.first_key_value(), self.bids.first_key_value()) {
            (Some((&ask, _)), Some((Reverse(bid), _))) => ask > *bid,
            _ => false,
        }
    }

    /// 最优卖价及其数量。
    pub fn best_ask(&self) -> Option<(f64, f64)> {
        self.asks
            .first_key_value()
            .map(|(&p, &q)| (self.decode_price(p), q))
    }

    /// 最优买价及其数量。
    pub fn best_bid(&self) -> Option<(f64, f64)> {
        self.bids
            .first_key_value()
            .map(|(Reverse(p), &q)| (self.decode_price(*p), q))
    }

    /// 买卖价差。
    pub fn spread(&self) -> Option<f64> {
        Some(self.best_ask()?.0 - self.best_bid()?.0)
    }

    /// 中间价格。
    pub fn mid_price(&self) -> Option<f64> {
        Some((self.best_ask()?.0 + self.best_bid()?.0) / 2.0)
    }

    /// 最优卖方档位。
    ///
    /// # 参数
    /// - `depth`：档位深度。
    /// - `decimal_places`：小数位数。
    ///
    /// # 返回
    /// - `Vec<(f64, f64)>`：最优卖方档位。
    pub fn best_asks(&self, depth: usize, decimal_places: u32) -> Vec<(f64, f64)> {
        let tick = self.tick_size(decimal_places);
        self.aggregate_levels(self.asks.iter().map(|(&p, &q)| (p, q)), depth, |price| {
            (price + tick - 1) / tick * tick
        })
    }

    /// 最优买方档位。
    ///
    /// # 参数
    /// - `depth`：档位深度。
    /// - `decimal_places`：小数位数。
    ///
    /// # 返回
    /// - `Vec<(f64, f64)>`：最优买方档位。
    pub fn best_bids(&self, depth: usize, decimal_places: u32) -> Vec<(f64, f64)> {
        let tick = self.tick_size(decimal_places);
        self.aggregate_levels(
            self.bids.iter().map(|(Reverse(p), &q)| (*p, q)),
            depth,
            |price| price / tick * tick,
        )
    }

    /// 编码价格。
    ///
    /// # 参数
    /// - `price`：价格。
    ///
    /// # 返回
    /// - `i64`：编码后的价格。
    fn encode_price(&self, price: f64) -> i64 {
        (price * self.multiplier).round() as i64
    }

    /// 解码价格。
    ///
    /// # 参数
    /// - `price`：编码后的价格。
    ///
    /// # 返回
    /// - `f64`：解码后的价格。
    fn decode_price(&self, price: i64) -> f64 {
        price as f64 / self.multiplier
    }

    /// 计算聚合步长。
    ///
    /// # 参数
    /// - `decimal_places`：小数位数。
    ///
    /// # 返回
    /// - `i64`：聚合步长。
    fn tick_size(&self, decimal_places: u32) -> i64 {
        10_i64.pow(self.decimal_places.saturating_sub(decimal_places))
    }

    /// 聚合档位。
    ///
    /// # 参数
    /// - `iter`：档位迭代器。
    /// - `depth`：档位深度。
    /// - `bucket_key`：聚合函数。
    ///
    /// # 返回
    /// - `Vec<(f64, f64)>`：聚合后的档位。
    fn aggregate_levels(
        &self,
        iter: impl Iterator<Item = (i64, f64)>,
        depth: usize,
        bucket_key: impl Fn(i64) -> i64,
    ) -> Vec<(f64, f64)> {
        let mut result: Vec<(f64, f64)> = Vec::with_capacity(depth);
        for (raw_price, qty) in iter {
            if qty.abs() < f64::EPSILON {
                continue;
            }
            let price = self.decode_price(bucket_key(raw_price));
            match result.last_mut() {
                Some(last) if last.0 == price => last.1 += qty,
                _ => {
                    if result.len() == depth {
                        break;
                    }
                    result.push((price, qty));
                }
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 构造一个已就绪的订单薄（BatchStart → updates → BatchEnd）。
    fn make_book(bids: &[(f64, f64)], asks: &[(f64, f64)]) -> OrderBook {
        let mut book = OrderBook::new(8);
        book.update(bids, asks);
        book
    }

    // --- ready 状态 ---

    #[test]
    fn not_ready_returns_empty() {
        let book = OrderBook::new(8);
        assert!(book.best_asks(10, 2).is_empty());
        assert!(book.best_bids(10, 2).is_empty());
    }

    // --- Clear ---

    #[test]
    fn clear_empties_book() {
        let mut book = make_book(&[(100.0, 1.0)], &[(101.0, 1.0)]);
        book.clear();
        assert!(book.best_asks(10, 8).is_empty());
        assert!(book.best_bids(10, 8).is_empty());
    }

    // --- 排序方向 ---

    #[test]
    fn asks_ascending_best_first() {
        let book = make_book(&[], &[(103.0, 1.0), (101.0, 2.0), (102.0, 3.0)]);
        let asks = book.best_asks(3, 2);
        assert_eq!(asks.len(), 3);
        assert_eq!(asks[0].0, 101.0);
        assert_eq!(asks[1].0, 102.0);
        assert_eq!(asks[2].0, 103.0);
    }

    #[test]
    fn bids_descending_best_first() {
        let book = make_book(&[(98.0, 1.0), (100.0, 2.0), (99.0, 3.0)], &[]);
        let bids = book.best_bids(3, 2);
        assert_eq!(bids.len(), 3);
        assert_eq!(bids[0].0, 100.0);
        assert_eq!(bids[1].0, 99.0);
        assert_eq!(bids[2].0, 98.0);
    }

    // --- depth 限制 ---

    #[test]
    fn depth_limits_ask_levels() {
        let book = make_book(&[], &[(101.0, 1.0), (102.0, 1.0), (103.0, 1.0)]);
        assert_eq!(book.best_asks(2, 2).len(), 2);
    }

    #[test]
    fn depth_limits_bid_levels() {
        let book = make_book(&[(100.0, 1.0), (99.0, 1.0), (98.0, 1.0)], &[]);
        assert_eq!(book.best_bids(2, 2).len(), 2);
    }

    // --- 聚合：ask 向上取整，bid 向下取整 ---

    #[test]
    fn ask_aggregation_ceiling() {
        // 保留 6 位小数（tick = 100），100.00000001、100.00000099、100.00000100
        // ceiling 后均为 100.000001，三者应合并为一档
        let book = make_book(
            &[],
            &[
                (100.00000001, 1.0),
                (100.00000099, 2.0),
                (100.00000100, 0.5),
            ],
        );
        let asks = book.best_asks(10, 6);
        assert_eq!(asks.len(), 1);
        assert!((asks[0].0 - 100.000001).abs() < 1e-9);
        assert!((asks[0].1 - 3.5).abs() < 1e-9);
    }

    #[test]
    fn bid_aggregation_floor() {
        // 保留 6 位小数（tick = 100），100.00000001 和 100.00000099
        // floor 后均为 100.000000，两者应合并为一档
        let book = make_book(&[(100.00000001, 1.0), (100.00000099, 2.0)], &[]);
        let bids = book.best_bids(10, 6);
        assert_eq!(bids.len(), 1);
        assert!((bids[0].0 - 100.0).abs() < 1e-9);
        assert!((bids[0].1 - 3.0).abs() < 1e-9);
    }

    #[test]
    fn aggregation_splits_into_multiple_buckets() {
        let book = make_book(
            &[],
            &[
                (100.00000001, 1.0), // ceiling → 100.000001
                (100.00000100, 2.0), // ceiling → 100.000001（恰好在边界）
                (100.00000200, 3.0), // ceiling → 100.000002（下一个 bucket）
            ],
        );
        let asks = book.best_asks(10, 6);
        assert_eq!(asks.len(), 2);
        assert!((asks[0].1 - 3.0).abs() < 1e-9); // 100.000001 bucket
        assert!((asks[1].1 - 3.0).abs() < 1e-9); // 100.000002 bucket
    }

    // --- check_validity ---

    #[test]
    fn validity_normal() {
        // 正常状态：最低卖价 > 最高买价
        let book = make_book(&[(100.0, 1.0)], &[(101.0, 1.0)]);
        assert!(book.check_validity());
    }

    #[test]
    fn validity_crossed() {
        // 价格交叉：最低卖价 < 最高买价
        let book = make_book(&[(102.0, 1.0)], &[(101.0, 1.0)]);
        assert!(!book.check_validity());
    }

    #[test]
    fn validity_equal_prices() {
        // 买卖同价（锁定盘口）：最低卖价 == 最高买价，应视为无效
        let book = make_book(&[(100.0, 1.0)], &[(100.0, 1.0)]);
        assert!(!book.check_validity());
    }

    #[test]
    fn validity_empty_asks() {
        // 卖方为空，无法判断是否交叉，返回 false
        let book = make_book(&[(100.0, 1.0)], &[]);
        assert!(!book.check_validity());
    }

    #[test]
    fn validity_empty_bids() {
        // 买方为空，无法判断是否交叉，返回 false
        let book = make_book(&[], &[(101.0, 1.0)]);
        assert!(!book.check_validity());
    }

    #[test]
    fn validity_empty_book() {
        // 双边均空，返回 false
        let book = make_book(&[], &[]);
        assert!(!book.check_validity());
    }

    #[test]
    fn validity_uses_best_bid_not_worst() {
        // 只有最高买价（100.0）需要低于最低卖价，其他买价不影响结果
        let book = make_book(&[(100.0, 1.0), (99.0, 1.0), (98.0, 1.0)], &[(101.0, 1.0)]);
        assert!(book.check_validity());
    }

    #[test]
    fn validity_crossed_detected_by_best_bid() {
        // 最高买价（102.0）超过最低卖价（101.0），即使其他买价正常也应检出交叉
        let book = make_book(&[(102.0, 1.0), (99.0, 1.0)], &[(101.0, 1.0)]);
        assert!(!book.check_validity());
    }

    // --- 零量档位 ---

    #[test]
    fn zero_qty_entry_is_skipped() {
        let book = make_book(&[], &[(101.0, 0.0), (102.0, 1.5)]);
        let asks = book.best_asks(10, 2);
        assert_eq!(asks.len(), 1);
        assert_eq!(asks[0].0, 102.0);
    }

    // --- spread ---

    #[test]
    fn spread_normal() {
        let book = make_book(&[(100.0, 1.0)], &[(101.0, 1.0)]);
        assert_eq!(book.spread(), Some(1.0));
    }

    #[test]
    fn spread_empty() {
        let book = make_book(&[], &[]);
        assert_eq!(book.spread(), None);
    }

    #[test]
    fn spread_empty_asks() {
        let book = make_book(&[], &[(101.0, 1.0)]);
        assert_eq!(book.spread(), None);
    }

    #[test]
    fn spread_empty_bids() {
        let book = make_book(&[(100.0, 1.0)], &[]);
        assert_eq!(book.spread(), None);
    }

    #[test]
    fn spread_empty_book() {
        let book = make_book(&[], &[]);
        assert_eq!(book.spread(), None);
    }

    // --- mid_price ---

    #[test]
    fn mid_price_normal() {
        let book = make_book(&[(100.0, 1.0)], &[(101.0, 1.0)]);
        assert_eq!(book.mid_price(), Some(100.5));
    }

    #[test]
    fn mid_price_empty() {
        let book = make_book(&[], &[]);
        assert_eq!(book.mid_price(), None);
    }

    #[test]
    fn mid_price_empty_asks() {
        let book = make_book(&[], &[(101.0, 1.0)]);
        assert_eq!(book.mid_price(), None);
    }

    #[test]
    fn mid_price_empty_bids() {
        let book = make_book(&[(100.0, 1.0)], &[]);
        assert_eq!(book.mid_price(), None);
    }

    #[test]
    fn mid_price_empty_book() {
        let book = make_book(&[], &[]);
        assert_eq!(book.mid_price(), None);
    }
}
