use crate::{Label, NativeHistogram, Value};

pub const PROMETHEUS_STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;

#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    pub metric: String,
    pub labels: Vec<Label>,
    pub timestamp: i64,
    pub value: f64,
    pub histogram: Option<Box<NativeHistogram>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Series {
    pub metric: String,
    pub labels: Vec<Label>,
    pub samples: Vec<(i64, f64)>,
    pub histograms: Vec<(i64, Box<NativeHistogram>)>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PromqlValue {
    Scalar(f64, i64),
    InstantVector(Vec<Sample>),
    RangeVector(Vec<Series>),
    String(String, i64),
}

impl PromqlValue {
    pub fn as_scalar(&self) -> Option<(f64, i64)> {
        match self {
            Self::Scalar(v, t) => Some((*v, *t)),
            _ => None,
        }
    }

    pub fn as_instant_vector(&self) -> Option<&[Sample]> {
        match self {
            Self::InstantVector(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_range_vector(&self) -> Option<&[Series]> {
        match self {
            Self::RangeVector(v) => Some(v),
            _ => None,
        }
    }
}

pub fn value_to_f64(value: &Value) -> f64 {
    match value {
        Value::F64(v) => *v,
        Value::I64(v) => *v as f64,
        Value::U64(v) => *v as f64,
        Value::Bool(v) => {
            if *v {
                1.0
            } else {
                0.0
            }
        }
        Value::Bytes(_) | Value::String(_) | Value::Histogram(_) => f64::NAN,
    }
}

pub fn is_stale_nan_value(value: &Value) -> bool {
    matches!(value, Value::F64(v) if v.to_bits() == PROMETHEUS_STALE_NAN_BITS)
}

#[derive(Debug, Clone, PartialEq)]
pub struct HistogramBucket {
    pub lower: f64,
    pub upper: f64,
    pub count: f64,
    pub lower_inclusive: bool,
    pub upper_inclusive: bool,
}

pub fn histogram_count_value(histogram: &NativeHistogram) -> f64 {
    histogram
        .count
        .as_ref()
        .map(crate::HistogramCount::as_f64)
        .unwrap_or_else(|| {
            histogram_zero_count_value(histogram)
                + decoded_bucket_values(
                    &histogram.negative_spans,
                    &histogram.negative_deltas,
                    &histogram.negative_counts,
                )
                .unwrap_or_default()
                .into_iter()
                .map(|(_, count)| count)
                .sum::<f64>()
                + decoded_bucket_values(
                    &histogram.positive_spans,
                    &histogram.positive_deltas,
                    &histogram.positive_counts,
                )
                .unwrap_or_default()
                .into_iter()
                .map(|(_, count)| count)
                .sum::<f64>()
        })
}

pub fn histogram_zero_count_value(histogram: &NativeHistogram) -> f64 {
    histogram
        .zero_count
        .as_ref()
        .map(crate::HistogramCount::as_f64)
        .unwrap_or(0.0)
}

pub fn histogram_layout_compatible(lhs: &NativeHistogram, rhs: &NativeHistogram) -> bool {
    lhs.schema == rhs.schema
        && lhs.zero_threshold == rhs.zero_threshold
        && lhs.custom_values == rhs.custom_values
        && lhs.negative_spans == rhs.negative_spans
        && lhs.positive_spans == rhs.positive_spans
}

pub fn histogram_add(
    lhs: &NativeHistogram,
    rhs: &NativeHistogram,
) -> Result<NativeHistogram, String> {
    combine_histograms(lhs, rhs, |left, right| left + right)
}

pub fn histogram_sub(
    lhs: &NativeHistogram,
    rhs: &NativeHistogram,
) -> Result<NativeHistogram, String> {
    combine_histograms(lhs, rhs, |left, right| left - right)
}

pub fn histogram_scale(
    histogram: &NativeHistogram,
    factor: f64,
) -> Result<NativeHistogram, String> {
    if !histogram.custom_values.is_empty() {
        return Err("custom bucket native histograms are not supported yet".to_string());
    }

    let negative_counts = decoded_bucket_values(
        &histogram.negative_spans,
        &histogram.negative_deltas,
        &histogram.negative_counts,
    )?
    .into_iter()
    .map(|(_, count)| count * factor)
    .collect();
    let positive_counts = decoded_bucket_values(
        &histogram.positive_spans,
        &histogram.positive_deltas,
        &histogram.positive_counts,
    )?
    .into_iter()
    .map(|(_, count)| count * factor)
    .collect();

    Ok(NativeHistogram {
        count: Some(crate::HistogramCount::Float(
            histogram_count_value(histogram) * factor,
        )),
        sum: histogram.sum * factor,
        schema: histogram.schema,
        zero_threshold: histogram.zero_threshold,
        zero_count: Some(crate::HistogramCount::Float(
            histogram_zero_count_value(histogram) * factor,
        )),
        negative_spans: histogram.negative_spans.clone(),
        negative_deltas: Vec::new(),
        negative_counts,
        positive_spans: histogram.positive_spans.clone(),
        positive_deltas: Vec::new(),
        positive_counts,
        reset_hint: histogram.reset_hint,
        custom_values: histogram.custom_values.clone(),
    })
}

pub fn histogram_counter_reset_detected(
    previous: &NativeHistogram,
    current: &NativeHistogram,
) -> Result<bool, String> {
    if !histogram_layout_compatible(previous, current) {
        return Err(
            "native histogram schema/layout changes across a single range are not supported yet"
                .to_string(),
        );
    }

    if matches!(current.reset_hint, crate::HistogramResetHint::Yes) {
        return Ok(true);
    }

    if histogram_count_value(current) < histogram_count_value(previous)
        || histogram_zero_count_value(current) < histogram_zero_count_value(previous)
        || current.sum < previous.sum
    {
        return Ok(true);
    }

    let previous_negative = decoded_bucket_values(
        &previous.negative_spans,
        &previous.negative_deltas,
        &previous.negative_counts,
    )?;
    let current_negative = decoded_bucket_values(
        &current.negative_spans,
        &current.negative_deltas,
        &current.negative_counts,
    )?;
    if current_negative
        .iter()
        .zip(previous_negative.iter())
        .any(|((_, current_count), (_, previous_count))| current_count < previous_count)
    {
        return Ok(true);
    }

    let previous_positive = decoded_bucket_values(
        &previous.positive_spans,
        &previous.positive_deltas,
        &previous.positive_counts,
    )?;
    let current_positive = decoded_bucket_values(
        &current.positive_spans,
        &current.positive_deltas,
        &current.positive_counts,
    )?;
    Ok(current_positive
        .iter()
        .zip(previous_positive.iter())
        .any(|((_, current_count), (_, previous_count))| current_count < previous_count))
}

pub fn histogram_buckets(histogram: &NativeHistogram) -> Result<Vec<HistogramBucket>, String> {
    if !histogram.custom_values.is_empty() {
        return Err("custom bucket native histograms are not supported yet".to_string());
    }

    let mut buckets = Vec::new();
    let negative = decoded_bucket_values(
        &histogram.negative_spans,
        &histogram.negative_deltas,
        &histogram.negative_counts,
    )?;
    for (index, count) in negative.into_iter().rev() {
        let (lower, upper) = negative_bucket_bounds(histogram.schema, index)?;
        buckets.push(HistogramBucket {
            lower,
            upper,
            count,
            lower_inclusive: true,
            upper_inclusive: false,
        });
    }

    let zero_count = histogram_zero_count_value(histogram);
    if zero_count > 0.0 {
        buckets.push(HistogramBucket {
            lower: -histogram.zero_threshold,
            upper: histogram.zero_threshold,
            count: zero_count,
            lower_inclusive: true,
            upper_inclusive: true,
        });
    }

    let positive = decoded_bucket_values(
        &histogram.positive_spans,
        &histogram.positive_deltas,
        &histogram.positive_counts,
    )?;
    for (index, count) in positive {
        let (lower, upper) = positive_bucket_bounds(histogram.schema, index)?;
        buckets.push(HistogramBucket {
            lower,
            upper,
            count,
            lower_inclusive: false,
            upper_inclusive: true,
        });
    }

    buckets.sort_by(|left, right| {
        left.lower
            .total_cmp(&right.lower)
            .then_with(|| left.upper.total_cmp(&right.upper))
    });
    Ok(buckets)
}

/// Port of Prometheus' `HistogramQuantile` for exponential native histograms.
pub fn histogram_quantile_native(phi: f64, histogram: &NativeHistogram) -> Result<f64, String> {
    if phi < 0.0 {
        return Ok(f64::NEG_INFINITY);
    }
    if phi > 1.0 {
        return Ok(f64::INFINITY);
    }
    let total = histogram_count_value(histogram);
    if phi.is_nan() || total.is_nan() || total <= 0.0 {
        return Ok(f64::NAN);
    }

    let buckets = populated_buckets_with_zero_bucket_edges(histogram)?;
    let rank = phi * total;
    let mut count = 0.0;
    let mut found = None;
    for bucket in &buckets {
        count += bucket.count;
        found = Some(bucket);
        if count >= rank {
            break;
        }
    }
    let Some(bucket) = found else {
        return Ok(f64::NAN);
    };
    let count = count.min(total);
    if count < rank {
        // Only NaN observations are left above the last bucket.
        return Ok(bucket.upper);
    }

    let fraction = (rank - (count - bucket.count)) / bucket.count;
    if bucket.lower <= 0.0 && bucket.upper >= 0.0 {
        // The zero bucket is interpolated linearly.
        return Ok(bucket.lower + (bucket.upper - bucket.lower) * fraction);
    }
    // Exponential buckets are interpolated on a logarithmic scale.
    let log_lower = bucket.lower.abs().log2();
    let log_upper = bucket.upper.abs().log2();
    if bucket.lower > 0.0 {
        Ok((log_lower + (log_upper - log_lower) * fraction).exp2())
    } else {
        Ok(-(log_upper + (log_lower - log_upper) * (1.0 - fraction)).exp2())
    }
}

/// Port of Prometheus' `HistogramFraction` for exponential native histograms:
/// the estimated fraction of observations in `(lower, upper]`.
pub fn histogram_fraction_native(
    lower: f64,
    upper: f64,
    histogram: &NativeHistogram,
) -> Result<f64, String> {
    let total = histogram_count_value(histogram);
    if total.is_nan() || total <= 0.0 || lower.is_nan() || upper.is_nan() {
        return Ok(f64::NAN);
    }
    if lower >= upper {
        return Ok(0.0);
    }

    let buckets = populated_buckets_with_zero_bucket_edges(histogram)?;
    let rank_at = |value: f64| {
        let mut rank = 0.0;
        for bucket in &buckets {
            if bucket.lower >= value {
                return rank;
            }
            if bucket.upper > value {
                let fraction = if bucket.lower <= 0.0 && bucket.upper >= 0.0 {
                    (value - bucket.lower) / (bucket.upper - bucket.lower)
                } else {
                    let log_lower = bucket.lower.abs().log2();
                    let log_upper = bucket.upper.abs().log2();
                    let log_value = value.abs().log2();
                    if value > 0.0 {
                        (log_value - log_lower) / (log_upper - log_lower)
                    } else {
                        1.0 - (log_value - log_upper) / (log_lower - log_upper)
                    }
                };
                return rank + bucket.count * fraction;
            }
            rank += bucket.count;
        }
        rank
    };

    Ok((rank_at(upper).min(total) - rank_at(lower).min(total)) / total)
}

/// Standard variance of a native histogram, estimating each observation by
/// its bucket's geometric mean (zero in the zero bucket), as Prometheus does.
pub fn histogram_stdvar_native(histogram: &NativeHistogram) -> Result<f64, String> {
    let total = histogram_count_value(histogram);
    let mean = histogram.sum / total;
    let mut variance = 0.0;
    for bucket in histogram_buckets(histogram)? {
        if bucket.count == 0.0 {
            continue;
        }
        let value = if bucket.lower <= 0.0 && bucket.upper >= 0.0 {
            0.0
        } else if bucket.upper < 0.0 {
            -(bucket.upper * bucket.lower).sqrt()
        } else {
            (bucket.upper * bucket.lower).sqrt()
        };
        variance += bucket.count * (value - mean) * (value - mean);
    }
    Ok(variance / total)
}

/// The non-empty buckets in ascending order. When the zero bucket is the only
/// bucket on its side of zero, that side's edge is moved to zero.
fn populated_buckets_with_zero_bucket_edges(
    histogram: &NativeHistogram,
) -> Result<Vec<HistogramBucket>, String> {
    let buckets = histogram_buckets(histogram)?;
    let has_negative = buckets.iter().any(|bucket| bucket.upper < 0.0);
    let has_positive = buckets.iter().any(|bucket| bucket.lower > 0.0);
    Ok(buckets
        .into_iter()
        .filter(|bucket| bucket.count != 0.0)
        .map(|mut bucket| {
            if bucket.lower < 0.0 && bucket.upper > 0.0 {
                if has_positive && !has_negative {
                    bucket.lower = 0.0;
                } else if has_negative && !has_positive {
                    bucket.upper = 0.0;
                }
            }
            bucket
        })
        .collect())
}

fn combine_histograms(
    lhs: &NativeHistogram,
    rhs: &NativeHistogram,
    combine: impl Fn(f64, f64) -> f64,
) -> Result<NativeHistogram, String> {
    if !histogram_layout_compatible(lhs, rhs) {
        return Err(
            "native histogram schema/layout changes across operands are not supported yet"
                .to_string(),
        );
    }
    if !lhs.custom_values.is_empty() {
        return Err("custom bucket native histograms are not supported yet".to_string());
    }

    let negative_counts = decode_and_combine_buckets(
        &lhs.negative_spans,
        &lhs.negative_deltas,
        &lhs.negative_counts,
        &rhs.negative_spans,
        &rhs.negative_deltas,
        &rhs.negative_counts,
        &combine,
    )?;
    let positive_counts = decode_and_combine_buckets(
        &lhs.positive_spans,
        &lhs.positive_deltas,
        &lhs.positive_counts,
        &rhs.positive_spans,
        &rhs.positive_deltas,
        &rhs.positive_counts,
        &combine,
    )?;

    Ok(NativeHistogram {
        count: Some(crate::HistogramCount::Float(combine(
            histogram_count_value(lhs),
            histogram_count_value(rhs),
        ))),
        sum: combine(lhs.sum, rhs.sum),
        schema: lhs.schema,
        zero_threshold: lhs.zero_threshold,
        zero_count: Some(crate::HistogramCount::Float(combine(
            histogram_zero_count_value(lhs),
            histogram_zero_count_value(rhs),
        ))),
        negative_spans: lhs.negative_spans.clone(),
        negative_deltas: Vec::new(),
        negative_counts,
        positive_spans: lhs.positive_spans.clone(),
        positive_deltas: Vec::new(),
        positive_counts,
        reset_hint: combine_reset_hints(lhs.reset_hint, rhs.reset_hint),
        custom_values: lhs.custom_values.clone(),
    })
}

fn decode_and_combine_buckets(
    lhs_spans: &[crate::HistogramBucketSpan],
    lhs_deltas: &[i64],
    lhs_counts: &[f64],
    rhs_spans: &[crate::HistogramBucketSpan],
    rhs_deltas: &[i64],
    rhs_counts: &[f64],
    combine: &impl Fn(f64, f64) -> f64,
) -> Result<Vec<f64>, String> {
    let lhs = decoded_bucket_values(lhs_spans, lhs_deltas, lhs_counts)?;
    let rhs = decoded_bucket_values(rhs_spans, rhs_deltas, rhs_counts)?;
    Ok(lhs
        .into_iter()
        .zip(rhs)
        .map(|((_, left), (_, right))| combine(left, right))
        .collect())
}

fn decoded_bucket_values(
    spans: &[crate::HistogramBucketSpan],
    deltas: &[i64],
    counts: &[f64],
) -> Result<Vec<(i32, f64)>, String> {
    let bucket_count = spans.iter().map(|span| span.length as usize).sum::<usize>();

    if !counts.is_empty() && !deltas.is_empty() {
        return Err(
            "native histogram cannot use integer deltas and float bucket counts at the same time"
                .to_string(),
        );
    }
    if counts.is_empty() && deltas.is_empty() {
        if bucket_count == 0 {
            return Ok(Vec::new());
        }
        return Err("native histogram bucket layout is missing encoded counts".to_string());
    }

    let raw_counts = if !counts.is_empty() {
        if counts.len() != bucket_count {
            return Err(format!(
                "native histogram bucket count mismatch: expected {bucket_count}, got {}",
                counts.len()
            ));
        }
        counts.to_vec()
    } else {
        if deltas.len() != bucket_count {
            return Err(format!(
                "native histogram bucket delta mismatch: expected {bucket_count}, got {}",
                deltas.len()
            ));
        }
        let mut out = Vec::with_capacity(bucket_count);
        let mut previous = 0.0;
        for delta in deltas {
            let current = previous + *delta as f64;
            out.push(current);
            previous = current;
        }
        out
    };

    let mut values = Vec::with_capacity(bucket_count);
    let mut next_index = None::<i32>;
    let mut count_iter = raw_counts.into_iter();
    for span in spans {
        let start = match next_index {
            Some(previous_end) => previous_end.saturating_add(span.offset),
            None => span.offset,
        };
        for idx in 0..span.length {
            values.push((
                start.saturating_add(i32::try_from(idx).unwrap_or(i32::MAX)),
                count_iter.next().unwrap_or_default(),
            ));
        }
        next_index = Some(start.saturating_add(i32::try_from(span.length).unwrap_or(i32::MAX)));
    }

    Ok(values)
}

// As in Prometheus, positive bucket `index` covers `(base^(index-1), base^index]`.
fn positive_bucket_bounds(schema: i32, index: i32) -> Result<(f64, f64), String> {
    Ok((
        bucket_boundary(schema, index - 1)?,
        bucket_boundary(schema, index)?,
    ))
}

fn negative_bucket_bounds(schema: i32, index: i32) -> Result<(f64, f64), String> {
    let (lower, upper) = positive_bucket_bounds(schema, index)?;
    Ok((-upper, -lower))
}

fn bucket_boundary(schema: i32, index: i32) -> Result<f64, String> {
    let growth = 2f64.powf(2f64.powi(-schema));
    let value = growth.powi(index);
    if !value.is_finite() {
        return Err(format!(
            "native histogram bucket schema {schema} cannot be represented as f64"
        ));
    }
    Ok(value)
}

fn combine_reset_hints(
    lhs: crate::HistogramResetHint,
    rhs: crate::HistogramResetHint,
) -> crate::HistogramResetHint {
    match (lhs, rhs) {
        (crate::HistogramResetHint::Gauge, crate::HistogramResetHint::Gauge) => {
            crate::HistogramResetHint::Gauge
        }
        (crate::HistogramResetHint::No, crate::HistogramResetHint::No) => {
            crate::HistogramResetHint::No
        }
        (crate::HistogramResetHint::Yes, _) | (_, crate::HistogramResetHint::Yes) => {
            crate::HistogramResetHint::Yes
        }
        _ => crate::HistogramResetHint::Unknown,
    }
}

impl Sample {
    pub fn from_float(metric: String, labels: Vec<Label>, timestamp: i64, value: f64) -> Self {
        Self {
            metric,
            labels,
            timestamp,
            value,
            histogram: None,
        }
    }

    pub fn from_histogram(
        metric: String,
        labels: Vec<Label>,
        timestamp: i64,
        histogram: NativeHistogram,
    ) -> Self {
        Self {
            metric,
            labels,
            timestamp,
            value: f64::NAN,
            histogram: Some(Box::new(histogram)),
        }
    }

    pub fn value_type_name(&self) -> &'static str {
        if self.histogram.is_some() {
            "histogram"
        } else {
            "float"
        }
    }

    pub fn histogram(&self) -> Option<&NativeHistogram> {
        self.histogram.as_deref()
    }
}

impl Series {
    pub fn new(metric: String, labels: Vec<Label>) -> Self {
        Self {
            metric,
            labels,
            samples: Vec::new(),
            histograms: Vec::new(),
        }
    }

    pub fn value_type_name(&self) -> &'static str {
        match (self.samples.is_empty(), self.histograms.is_empty()) {
            (false, true) => "float",
            (true, false) => "histogram",
            (false, false) => "mixed",
            (true, true) => "empty",
        }
    }
}
