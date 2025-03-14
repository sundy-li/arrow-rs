// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Defines cast kernels for `ArrayRef`, to convert `Array`s between
//! supported datatypes.
//!
//! Example:
//!
//! ```
//! use arrow_array::*;
//! use arrow_cast::cast;
//! use arrow_schema::DataType;
//! use std::sync::Arc;
//!
//! let a = Int32Array::from(vec![5, 6, 7]);
//! let array = Arc::new(a) as ArrayRef;
//! let b = cast(&array, &DataType::Float64).unwrap();
//! let c = b.as_any().downcast_ref::<Float64Array>().unwrap();
//! assert_eq!(5.0, c.value(0));
//! assert_eq!(6.0, c.value(1));
//! assert_eq!(7.0, c.value(2));
//! ```

use chrono::{NaiveTime, TimeZone, Timelike, Utc};
use std::cmp::Ordering;
use std::sync::Arc;

use crate::display::{array_value_to_string, ArrayFormatter, FormatOptions};
use crate::parse::{
    parse_interval_day_time, parse_interval_month_day_nano, parse_interval_year_month,
    string_to_datetime,
};
use arrow_array::{
    builder::*, cast::*, temporal_conversions::*, timezone::Tz, types::*, *,
};
use arrow_buffer::{i256, ArrowNativeType, Buffer, MutableBuffer};
use arrow_data::ArrayData;
use arrow_schema::*;
use arrow_select::take::take;
use num::cast::AsPrimitive;
use num::{NumCast, ToPrimitive};

/// CastOptions provides a way to override the default cast behaviors
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CastOptions {
    /// how to handle cast failures, either return NULL (safe=true) or return ERR (safe=false)
    pub safe: bool,
}

pub const DEFAULT_CAST_OPTIONS: CastOptions = CastOptions { safe: true };

/// Return true if a value of type `from_type` can be cast into a
/// value of `to_type`. Note that such as cast may be lossy.
///
/// If this function returns true to stay consistent with the `cast` kernel below.
pub fn can_cast_types(from_type: &DataType, to_type: &DataType) -> bool {
    use self::DataType::*;
    if from_type == to_type {
        return true;
    }

    match (from_type, to_type) {
        (
            Null,
            Boolean
            | Int8
            | UInt8
            | Int16
            | UInt16
            | Int32
            | UInt32
            | Float32
            | Date32
            | Time32(_)
            | Int64
            | UInt64
            | Float64
            | Date64
            | Timestamp(_, _)
            | Time64(_)
            | Duration(_)
            | Interval(_)
            | FixedSizeBinary(_)
            | Binary
            | Utf8
            | LargeBinary
            | LargeUtf8
            | List(_)
            | LargeList(_)
            | FixedSizeList(_, _)
            | Struct(_)
            | Map(_, _)
            | Dictionary(_, _)
        ) => true,
        // Dictionary/List conditions should be put in front of others
        (Dictionary(_, from_value_type), Dictionary(_, to_value_type)) => {
            can_cast_types(from_value_type, to_value_type)
        }
        (Dictionary(_, value_type), _) => can_cast_types(value_type, to_type),
        (_, Dictionary(_, value_type)) => can_cast_types(from_type, value_type),
        (LargeList(list_from), LargeList(list_to)) => {
            can_cast_types(list_from.data_type(), list_to.data_type())
        }
        (List(list_from), List(list_to)) => {
            can_cast_types(list_from.data_type(), list_to.data_type())
        }
        (List(list_from), LargeList(list_to)) => {
            list_from.data_type() == list_to.data_type()
        }
        (LargeList(list_from), List(list_to)) => {
            list_from.data_type() == list_to.data_type()
        }
        (List(list_from) | LargeList(list_from), Utf8 | LargeUtf8) => can_cast_types(list_from.data_type(), to_type),
        (List(_), _) => false,
        (_, List(list_to)) => can_cast_types(from_type, list_to.data_type()),
        (_, LargeList(list_to)) => can_cast_types(from_type, list_to.data_type()),
        // cast one decimal type to another decimal type
        (Decimal128(_, _), Decimal128(_, _)) => true,
        (Decimal256(_, _), Decimal256(_, _)) => true,
        (Decimal128(_, _), Decimal256(_, _)) => true,
        (Decimal256(_, _), Decimal128(_, _)) => true,
        // unsigned integer to decimal
        (UInt8 | UInt16 | UInt32 | UInt64, Decimal128(_, _)) |
        (UInt8 | UInt16 | UInt32 | UInt64, Decimal256(_, _)) |
        // signed numeric to decimal
        (Null | Int8 | Int16 | Int32 | Int64 | Float32 | Float64, Decimal128(_, _)) |
        (Null | Int8 | Int16 | Int32 | Int64 | Float32 | Float64, Decimal256(_, _)) |
        // decimal to unsigned numeric
        (Decimal128(_, _), UInt8 | UInt16 | UInt32 | UInt64) |
        (Decimal256(_, _), UInt8 | UInt16 | UInt32 | UInt64) |
        // decimal to signed numeric
        (Decimal128(_, _), Null | Int8 | Int16 | Int32 | Int64 | Float32 | Float64) |
        (Decimal256(_, _), Null | Int8 | Int16 | Int32 | Int64 | Float32 | Float64) => true,
        // decimal to Utf8
        (Decimal128(_, _), Utf8 | LargeUtf8) => true,
        (Decimal256(_, _), Utf8 | LargeUtf8) => true,
        // Utf8 to decimal
        (Utf8 | LargeUtf8, Decimal128(_, _)) => true,
        (Utf8 | LargeUtf8, Decimal256(_, _)) => true,
        (Decimal128(_, _), _) => false,
        (_, Decimal128(_, _)) => false,
        (Decimal256(_, _), _) => false,
        (_, Decimal256(_, _)) => false,
        (Struct(_), _) => false,
        (_, Struct(_)) => false,
        (_, Boolean) => DataType::is_numeric(from_type) || from_type == &Utf8 || from_type == &LargeUtf8,
        (Boolean, _) => DataType::is_numeric(to_type) || to_type == &Utf8 || to_type == &LargeUtf8,

        (Binary, LargeBinary | Utf8 | LargeUtf8 | FixedSizeBinary(_)) => true,
        (LargeBinary, Binary | Utf8 | LargeUtf8 | FixedSizeBinary(_)) => true,
        (FixedSizeBinary(_), Binary | LargeBinary) => true,
        (Utf8,
            Binary
            | LargeBinary
            | LargeUtf8
            | Date32
            | Date64
            | Time32(TimeUnit::Second)
            | Time32(TimeUnit::Millisecond)
            | Time64(TimeUnit::Microsecond)
            | Time64(TimeUnit::Nanosecond)
            | Timestamp(TimeUnit::Second, _)
            | Timestamp(TimeUnit::Millisecond, _)
            | Timestamp(TimeUnit::Microsecond, _)
            | Timestamp(TimeUnit::Nanosecond, _)
            | Interval(_)
        ) => true,
        (Utf8, _) => to_type.is_numeric() && to_type != &Float16,
        (LargeUtf8,
            Binary
            | LargeBinary
            | Utf8
            | Date32
            | Date64
            | Time32(TimeUnit::Second)
            | Time32(TimeUnit::Millisecond)
            | Time64(TimeUnit::Microsecond)
            | Time64(TimeUnit::Nanosecond)
            | Timestamp(TimeUnit::Second, _)
            | Timestamp(TimeUnit::Millisecond, _)
            | Timestamp(TimeUnit::Microsecond, _)
            | Timestamp(TimeUnit::Nanosecond, _)
            | Interval(_)
        ) => true,
        (LargeUtf8, _) => to_type.is_numeric() && to_type != &Float16,
        (_, Utf8 | LargeUtf8) => from_type.is_primitive(),

        // start numeric casts
        (
            UInt8,
            UInt16 | UInt32 | UInt64 | Int8 | Int16 | Int32 | Int64 | Float32 | Float64,
        ) => true,

        (
            UInt16,
            UInt8 | UInt32 | UInt64 | Int8 | Int16 | Int32 | Int64 | Float32 | Float64,
        ) => true,

        (
            UInt32,
            UInt8 | UInt16 | UInt64 | Int8 | Int16 | Int32 | Int64 | Float32 | Float64,
        ) => true,

        (
            UInt64,
            UInt8 | UInt16 | UInt32 | Int8 | Int16 | Int32 | Int64 | Float32 | Float64,
        ) => true,

        (
            Int8,
            UInt8 | UInt16 | UInt32 | UInt64 | Int16 | Int32 | Int64 | Float32 | Float64,
        ) => true,

        (
            Int16,
            UInt8 | UInt16 | UInt32 | UInt64 | Int8 | Int32 | Int64 | Float32 | Float64,
        ) => true,

        (
            Int32,
            UInt8 | UInt16 | UInt32 | UInt64 | Int8 | Int16 | Int64 | Float32 | Float64,
        ) => true,

        (
            Int64,
            UInt8 | UInt16 | UInt32 | UInt64 | Int8 | Int16 | Int32 | Float32 | Float64,
        ) => true,

        (
            Float32,
            UInt8 | UInt16 | UInt32 | UInt64 | Int8 | Int16 | Int32 | Int64 | Float64,
        ) => true,

        (
            Float64,
            UInt8 | UInt16 | UInt32 | UInt64 | Int8 | Int16 | Int32 | Int64 | Float32,
        ) => true,
        // end numeric casts

        // temporal casts
        (Int32, Date32 | Date64 | Time32(_)) => true,
        (Date32, Int32 | Int64) => true,
        (Time32(_), Int32) => true,
        (Int64, Date64 | Date32 | Time64(_)) => true,
        (Date64, Int64 | Int32) => true,
        (Time64(_), Int64) => true,
        (Date32, Date64) => true,
        (Date64, Date32) => true,
        (Time32(TimeUnit::Second), Time32(TimeUnit::Millisecond)) => true,
        (Time32(TimeUnit::Millisecond), Time32(TimeUnit::Second)) => true,
        (Time32(_), Time64(_)) => true,
        (Time64(TimeUnit::Microsecond), Time64(TimeUnit::Nanosecond)) => true,
        (Time64(TimeUnit::Nanosecond), Time64(TimeUnit::Microsecond)) => true,
        (Time64(_), Time32(to_unit)) => {
            matches!(to_unit, TimeUnit::Second | TimeUnit::Millisecond)
        }
        (Timestamp(_, _), Int64) => true,
        (Int64, Timestamp(_, _)) => true,
        (Date64, Timestamp(_, None)) => true,
        (Date32, Timestamp(_, None)) => true,
        (Timestamp(_, _),
            Timestamp(_, _)
            | Date32
            | Date64
            | Time32(TimeUnit::Second)
            | Time32(TimeUnit::Millisecond)
            | Time64(TimeUnit::Microsecond)
            | Time64(TimeUnit::Nanosecond)) => true,
        (Int64, Duration(_)) => true,
        (Duration(_), Int64) => true,
        (Interval(from_type), Int64) => {
            match from_type {
                IntervalUnit::YearMonth => true,
                IntervalUnit::DayTime => true,
                IntervalUnit::MonthDayNano => false, // Native type is i128
            }
        }
        (Int32, Interval(to_type)) => {
            match to_type {
                IntervalUnit::YearMonth => true,
                IntervalUnit::DayTime => false,
                IntervalUnit::MonthDayNano => false,
            }
        }
        (Int64, Interval(to_type)) => {
            match to_type {
                IntervalUnit::YearMonth => false,
                IntervalUnit::DayTime => true,
                IntervalUnit::MonthDayNano => false,
            }
        }
        (Duration(_), Interval(IntervalUnit::MonthDayNano)) => true,
        (Interval(IntervalUnit::MonthDayNano), Duration(_)) => true,
        (_, _) => false,
    }
}

/// Cast `array` to the provided data type and return a new Array with
/// type `to_type`, if possible.
///
/// Behavior:
/// * Boolean to Utf8: `true` => '1', `false` => `0`
/// * Utf8 to boolean: `true`, `yes`, `on`, `1` => `true`, `false`, `no`, `off`, `0` => `false`,
///   short variants are accepted, other strings return null or error
/// * Utf8 to numeric: strings that can't be parsed to numbers return null, float strings
///   in integer casts return null
/// * Numeric to boolean: 0 returns `false`, any other value returns `true`
/// * List to List: the underlying data type is cast
/// * Primitive to List: a list array with 1 value per slot is created
/// * Date32 and Date64: precision lost when going to higher interval
/// * Time32 and Time64: precision lost when going to higher interval
/// * Timestamp and Date{32|64}: precision lost when going to higher interval
/// * Temporal to/from backing primitive: zero-copy with data type change
/// * Casting from `float32/float64` to `Decimal(precision, scale)` rounds to the `scale` decimals
///   (i.e. casting 6.4999 to Decimal(10, 1) becomes 6.5). This is the breaking change from `26.0.0`.
///   It used to truncate it instead of round (i.e. outputs 6.4 instead)
///
/// Unsupported Casts
/// * To or from `StructArray`
/// * List to primitive
/// * Interval and duration
pub fn cast(array: &dyn Array, to_type: &DataType) -> Result<ArrayRef, ArrowError> {
    cast_with_options(array, to_type, &DEFAULT_CAST_OPTIONS)
}

fn cast_integer_to_decimal<
    T: ArrowPrimitiveType,
    D: DecimalType + ArrowPrimitiveType<Native = M>,
    M,
>(
    array: &PrimitiveArray<T>,
    precision: u8,
    scale: i8,
    base: M,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    <T as ArrowPrimitiveType>::Native: AsPrimitive<M>,
    M: ArrowNativeTypeOp,
{
    let scale_factor = base.pow_checked(scale.unsigned_abs() as u32).map_err(|_| {
        ArrowError::CastError(format!(
            "Cannot cast to {:?}({}, {}). The scale causes overflow.",
            D::PREFIX,
            precision,
            scale,
        ))
    })?;

    let array = if scale < 0 {
        match cast_options.safe {
            true => array.unary_opt::<_, D>(|v| {
                v.as_().div_checked(scale_factor).ok().and_then(|v| {
                    (D::validate_decimal_precision(v, precision).is_ok()).then_some(v)
                })
            }),
            false => array.try_unary::<_, D, _>(|v| {
                v.as_()
                    .div_checked(scale_factor)
                    .and_then(|v| D::validate_decimal_precision(v, precision).map(|_| v))
            })?,
        }
    } else {
        match cast_options.safe {
            true => array.unary_opt::<_, D>(|v| {
                v.as_().mul_checked(scale_factor).ok().and_then(|v| {
                    (D::validate_decimal_precision(v, precision).is_ok()).then_some(v)
                })
            }),
            false => array.try_unary::<_, D, _>(|v| {
                v.as_()
                    .mul_checked(scale_factor)
                    .and_then(|v| D::validate_decimal_precision(v, precision).map(|_| v))
            })?,
        }
    };

    Ok(Arc::new(array.with_precision_and_scale(precision, scale)?))
}

fn cast_floating_point_to_decimal128<T: ArrowPrimitiveType>(
    array: &PrimitiveArray<T>,
    precision: u8,
    scale: i8,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    <T as ArrowPrimitiveType>::Native: AsPrimitive<f64>,
{
    let mul = 10_f64.powi(scale as i32);

    if cast_options.safe {
        array
            .unary_opt::<_, Decimal128Type>(|v| (mul * v.as_()).round().to_i128())
            .with_precision_and_scale(precision, scale)
            .map(|a| Arc::new(a) as ArrayRef)
    } else {
        array
            .try_unary::<_, Decimal128Type, _>(|v| {
                (mul * v.as_()).round().to_i128().ok_or_else(|| {
                    ArrowError::CastError(format!(
                        "Cannot cast to {}({}, {}). Overflowing on {:?}",
                        Decimal128Type::PREFIX,
                        precision,
                        scale,
                        v
                    ))
                })
            })?
            .with_precision_and_scale(precision, scale)
            .map(|a| Arc::new(a) as ArrayRef)
    }
}

fn cast_floating_point_to_decimal256<T: ArrowPrimitiveType>(
    array: &PrimitiveArray<T>,
    precision: u8,
    scale: i8,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    <T as ArrowPrimitiveType>::Native: AsPrimitive<f64>,
{
    let mul = 10_f64.powi(scale as i32);

    if cast_options.safe {
        array
            .unary_opt::<_, Decimal256Type>(|v| i256::from_f64((v.as_() * mul).round()))
            .with_precision_and_scale(precision, scale)
            .map(|a| Arc::new(a) as ArrayRef)
    } else {
        array
            .try_unary::<_, Decimal256Type, _>(|v| {
                i256::from_f64((v.as_() * mul).round()).ok_or_else(|| {
                    ArrowError::CastError(format!(
                        "Cannot cast to {}({}, {}). Overflowing on {:?}",
                        Decimal256Type::PREFIX,
                        precision,
                        scale,
                        v
                    ))
                })
            })?
            .with_precision_and_scale(precision, scale)
            .map(|a| Arc::new(a) as ArrayRef)
    }
}

/// Cast the array from interval to duration
fn cast_interval_to_duration<D: ArrowTemporalType<Native = i64>>(
    array: &dyn Array,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let array = array
        .as_any()
        .downcast_ref::<IntervalMonthDayNanoArray>()
        .ok_or_else(|| {
            ArrowError::ComputeError(
                "Internal Error: Cannot cast interval to IntervalArray of expected type"
                    .to_string(),
            )
        })?;

    let scale = match D::DATA_TYPE {
        DataType::Duration(TimeUnit::Second) => 1_000_000_000,
        DataType::Duration(TimeUnit::Millisecond) => 1_000_000,
        DataType::Duration(TimeUnit::Microsecond) => 1_000,
        DataType::Duration(TimeUnit::Nanosecond) => 1,
        _ => unreachable!(),
    };

    if cast_options.safe {
        let iter = array.iter().map(|v| {
            v.and_then(|v| {
                let v = v / scale;
                if v > i64::MAX as i128 {
                    None
                } else {
                    Some(v as i64)
                }
            })
        });
        Ok(Arc::new(unsafe {
            PrimitiveArray::<D>::from_trusted_len_iter(iter)
        }))
    } else {
        let vec = array
            .iter()
            .map(|v| {
                v.map(|v| {
                    let v = v / scale;
                    if v > i64::MAX as i128 {
                        Err(ArrowError::ComputeError(format!(
                            "Cannot cast to {:?}. Overflowing on {:?}",
                            D::DATA_TYPE,
                            v
                        )))
                    } else {
                        Ok(v as i64)
                    }
                })
                .transpose()
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Arc::new(unsafe {
            PrimitiveArray::<D>::from_trusted_len_iter(vec.iter())
        }))
    }
}

/// Cast the array from duration and interval
fn cast_duration_to_interval<D: ArrowTemporalType<Native = i64>>(
    array: &dyn Array,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let array = array
        .as_any()
        .downcast_ref::<PrimitiveArray<D>>()
        .ok_or_else(|| {
            ArrowError::ComputeError(
                "Internal Error: Cannot cast duration to DurationArray of expected type"
                    .to_string(),
            )
        })?;

    let scale = match array.data_type() {
        DataType::Duration(TimeUnit::Second) => 1_000_000_000,
        DataType::Duration(TimeUnit::Millisecond) => 1_000_000,
        DataType::Duration(TimeUnit::Microsecond) => 1_000,
        DataType::Duration(TimeUnit::Nanosecond) => 1,
        _ => unreachable!(),
    };

    if cast_options.safe {
        let iter = array
            .iter()
            .map(|v| v.and_then(|v| v.checked_mul(scale).map(|v| v as i128)));
        Ok(Arc::new(unsafe {
            PrimitiveArray::<IntervalMonthDayNanoType>::from_trusted_len_iter(iter)
        }))
    } else {
        let vec = array
            .iter()
            .map(|v| {
                v.map(|v| {
                    if let Ok(v) = v.mul_checked(scale) {
                        Ok(v as i128)
                    } else {
                        Err(ArrowError::ComputeError(format!(
                            "Cannot cast to {:?}. Overflowing on {:?}",
                            IntervalMonthDayNanoType::DATA_TYPE,
                            v
                        )))
                    }
                })
                .transpose()
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Arc::new(unsafe {
            PrimitiveArray::<IntervalMonthDayNanoType>::from_trusted_len_iter(vec.iter())
        }))
    }
}

/// Cast the primitive array using [`PrimitiveArray::reinterpret_cast`]
fn cast_reinterpret_arrays<
    I: ArrowPrimitiveType,
    O: ArrowPrimitiveType<Native = I::Native>,
>(
    array: &dyn Array,
) -> Result<ArrayRef, ArrowError> {
    Ok(Arc::new(array.as_primitive::<I>().reinterpret_cast::<O>()))
}

fn cast_decimal_to_integer<D, T>(
    array: &dyn Array,
    base: D::Native,
    scale: i8,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    T: ArrowPrimitiveType,
    <T as ArrowPrimitiveType>::Native: NumCast,
    D: DecimalType + ArrowPrimitiveType,
    <D as ArrowPrimitiveType>::Native: ArrowNativeTypeOp + ToPrimitive,
{
    let array = array.as_primitive::<D>();

    let div: D::Native = base.pow_checked(scale as u32).map_err(|_| {
        ArrowError::CastError(format!(
            "Cannot cast to {:?}. The scale {} causes overflow.",
            D::PREFIX,
            scale,
        ))
    })?;

    let mut value_builder = PrimitiveBuilder::<T>::with_capacity(array.len());

    if cast_options.safe {
        for i in 0..array.len() {
            if array.is_null(i) {
                value_builder.append_null();
            } else {
                let v = array
                    .value(i)
                    .div_checked(div)
                    .ok()
                    .and_then(<T::Native as NumCast>::from::<D::Native>);

                value_builder.append_option(v);
            }
        }
    } else {
        for i in 0..array.len() {
            if array.is_null(i) {
                value_builder.append_null();
            } else {
                let v = array.value(i).div_checked(div)?;

                let value =
                    <T::Native as NumCast>::from::<D::Native>(v).ok_or_else(|| {
                        ArrowError::CastError(format!(
                            "value of {:?} is out of range {}",
                            v,
                            T::DATA_TYPE
                        ))
                    })?;

                value_builder.append_value(value);
            }
        }
    }
    Ok(Arc::new(value_builder.finish()))
}

// cast the decimal array to floating-point array
fn cast_decimal_to_float<D: DecimalType, T: ArrowPrimitiveType, F>(
    array: &dyn Array,
    op: F,
) -> Result<ArrayRef, ArrowError>
where
    F: Fn(D::Native) -> T::Native,
{
    let array = array.as_primitive::<D>();
    let array = array.unary::<_, T>(op);
    Ok(Arc::new(array))
}

// cast the List array to Utf8 array
macro_rules! cast_list_to_string {
    ($ARRAY:expr, $SIZE:ident) => {{
        let mut value_builder: GenericStringBuilder<$SIZE> = GenericStringBuilder::new();
        for i in 0..$ARRAY.len() {
            if $ARRAY.is_null(i) {
                value_builder.append_null();
            } else {
                value_builder.append_value(array_value_to_string($ARRAY, i)?);
            }
        }
        Ok(Arc::new(value_builder.finish()))
    }};
}

fn make_timestamp_array(
    array: &PrimitiveArray<Int64Type>,
    unit: TimeUnit,
    tz: Option<Arc<str>>,
) -> ArrayRef {
    match unit {
        TimeUnit::Second => Arc::new(
            array
                .reinterpret_cast::<TimestampSecondType>()
                .with_timezone_opt(tz),
        ),
        TimeUnit::Millisecond => Arc::new(
            array
                .reinterpret_cast::<TimestampMillisecondType>()
                .with_timezone_opt(tz),
        ),
        TimeUnit::Microsecond => Arc::new(
            array
                .reinterpret_cast::<TimestampMicrosecondType>()
                .with_timezone_opt(tz),
        ),
        TimeUnit::Nanosecond => Arc::new(
            array
                .reinterpret_cast::<TimestampNanosecondType>()
                .with_timezone_opt(tz),
        ),
    }
}

fn as_time_res_with_timezone<T: ArrowPrimitiveType>(
    v: i64,
    tz: Option<Tz>,
) -> Result<NaiveTime, ArrowError> {
    let time = match tz {
        Some(tz) => as_datetime_with_timezone::<T>(v, tz).map(|d| d.time()),
        None => as_datetime::<T>(v).map(|d| d.time()),
    };

    time.ok_or_else(|| {
        ArrowError::CastError(format!(
            "Failed to create naive time with {} {}",
            std::any::type_name::<T>(),
            v
        ))
    })
}

/// Cast `array` to the provided data type and return a new Array with
/// type `to_type`, if possible. It accepts `CastOptions` to allow consumers
/// to configure cast behavior.
///
/// Behavior:
/// * Boolean to Utf8: `true` => '1', `false` => `0`
/// * Utf8 to boolean: `true`, `yes`, `on`, `1` => `true`, `false`, `no`, `off`, `0` => `false`,
///   short variants are accepted, other strings return null or error
/// * Utf8 to numeric: strings that can't be parsed to numbers return null, float strings
///   in integer casts return null
/// * Numeric to boolean: 0 returns `false`, any other value returns `true`
/// * List to List: the underlying data type is cast
/// * Primitive to List: a list array with 1 value per slot is created
/// * Date32 and Date64: precision lost when going to higher interval
/// * Time32 and Time64: precision lost when going to higher interval
/// * Timestamp and Date{32|64}: precision lost when going to higher interval
/// * Temporal to/from backing primitive: zero-copy with data type change
///
/// Unsupported Casts
/// * To or from `StructArray`
/// * List to primitive
pub fn cast_with_options(
    array: &dyn Array,
    to_type: &DataType,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    use DataType::*;
    let from_type = array.data_type();
    // clone array if types are the same
    if from_type == to_type {
        return Ok(make_array(array.to_data()));
    }
    match (from_type, to_type) {
        (
            Null,
            Boolean
            | Int8
            | UInt8
            | Int16
            | UInt16
            | Int32
            | UInt32
            | Float32
            | Date32
            | Time32(_)
            | Int64
            | UInt64
            | Float64
            | Date64
            | Timestamp(_, _)
            | Time64(_)
            | Duration(_)
            | Interval(_)
            | FixedSizeBinary(_)
            | Binary
            | Utf8
            | LargeBinary
            | LargeUtf8
            | List(_)
            | LargeList(_)
            | FixedSizeList(_, _)
            | Struct(_)
            | Map(_, _)
            | Dictionary(_, _),
        ) => Ok(new_null_array(to_type, array.len())),
        (Dictionary(index_type, _), _) => match **index_type {
            Int8 => dictionary_cast::<Int8Type>(array, to_type, cast_options),
            Int16 => dictionary_cast::<Int16Type>(array, to_type, cast_options),
            Int32 => dictionary_cast::<Int32Type>(array, to_type, cast_options),
            Int64 => dictionary_cast::<Int64Type>(array, to_type, cast_options),
            UInt8 => dictionary_cast::<UInt8Type>(array, to_type, cast_options),
            UInt16 => dictionary_cast::<UInt16Type>(array, to_type, cast_options),
            UInt32 => dictionary_cast::<UInt32Type>(array, to_type, cast_options),
            UInt64 => dictionary_cast::<UInt64Type>(array, to_type, cast_options),
            _ => Err(ArrowError::CastError(format!(
                "Casting from dictionary type {from_type:?} to {to_type:?} not supported",
            ))),
        },
        (_, Dictionary(index_type, value_type)) => match **index_type {
            Int8 => cast_to_dictionary::<Int8Type>(array, value_type, cast_options),
            Int16 => cast_to_dictionary::<Int16Type>(array, value_type, cast_options),
            Int32 => cast_to_dictionary::<Int32Type>(array, value_type, cast_options),
            Int64 => cast_to_dictionary::<Int64Type>(array, value_type, cast_options),
            UInt8 => cast_to_dictionary::<UInt8Type>(array, value_type, cast_options),
            UInt16 => cast_to_dictionary::<UInt16Type>(array, value_type, cast_options),
            UInt32 => cast_to_dictionary::<UInt32Type>(array, value_type, cast_options),
            UInt64 => cast_to_dictionary::<UInt64Type>(array, value_type, cast_options),
            _ => Err(ArrowError::CastError(format!(
                "Casting from type {from_type:?} to dictionary type {to_type:?} not supported",
            ))),
        },
        (List(_), List(ref to)) => {
            cast_list_inner::<i32>(array, to, to_type, cast_options)
        }
        (LargeList(_), LargeList(ref to)) => {
            cast_list_inner::<i64>(array, to, to_type, cast_options)
        }
        (List(list_from), LargeList(list_to)) => {
            if list_to.data_type() != list_from.data_type() {
                Err(ArrowError::CastError(
                    "cannot cast list to large-list with different child data".into(),
                ))
            } else {
                cast_list_container::<i32, i64>(array, cast_options)
            }
        }
        (LargeList(list_from), List(list_to)) => {
            if list_to.data_type() != list_from.data_type() {
                Err(ArrowError::CastError(
                    "cannot cast large-list to list with different child data".into(),
                ))
            } else {
                cast_list_container::<i64, i32>(array, cast_options)
            }
        }
        (List(_) | LargeList(_), _) => match to_type {
            Utf8 => cast_list_to_string!(array, i32),
            LargeUtf8 => cast_list_to_string!(array, i64),
            _ => Err(ArrowError::CastError(
                "Cannot cast list to non-list data types".to_string(),
            )),
        },
        (_, List(ref to)) => {
            cast_primitive_to_list::<i32>(array, to, to_type, cast_options)
        }
        (_, LargeList(ref to)) => {
            cast_primitive_to_list::<i64>(array, to, to_type, cast_options)
        }
        (Decimal128(_, s1), Decimal128(p2, s2)) => {
            cast_decimal_to_decimal_same_type::<Decimal128Type>(
                array.as_primitive(),
                *s1,
                *p2,
                *s2,
                cast_options,
            )
        }
        (Decimal256(_, s1), Decimal256(p2, s2)) => {
            cast_decimal_to_decimal_same_type::<Decimal256Type>(
                array.as_primitive(),
                *s1,
                *p2,
                *s2,
                cast_options,
            )
        }
        (Decimal128(_, s1), Decimal256(p2, s2)) => {
            cast_decimal_to_decimal::<Decimal128Type, Decimal256Type>(
                array.as_primitive(),
                *s1,
                *p2,
                *s2,
                cast_options,
            )
        }
        (Decimal256(_, s1), Decimal128(p2, s2)) => {
            cast_decimal_to_decimal::<Decimal256Type, Decimal128Type>(
                array.as_primitive(),
                *s1,
                *p2,
                *s2,
                cast_options,
            )
        }
        (Decimal128(_, scale), _) => {
            // cast decimal to other type
            match to_type {
                UInt8 => cast_decimal_to_integer::<Decimal128Type, UInt8Type>(
                    array,
                    10_i128,
                    *scale,
                    cast_options,
                ),
                UInt16 => cast_decimal_to_integer::<Decimal128Type, UInt16Type>(
                    array,
                    10_i128,
                    *scale,
                    cast_options,
                ),
                UInt32 => cast_decimal_to_integer::<Decimal128Type, UInt32Type>(
                    array,
                    10_i128,
                    *scale,
                    cast_options,
                ),
                UInt64 => cast_decimal_to_integer::<Decimal128Type, UInt64Type>(
                    array,
                    10_i128,
                    *scale,
                    cast_options,
                ),
                Int8 => cast_decimal_to_integer::<Decimal128Type, Int8Type>(
                    array,
                    10_i128,
                    *scale,
                    cast_options,
                ),
                Int16 => cast_decimal_to_integer::<Decimal128Type, Int16Type>(
                    array,
                    10_i128,
                    *scale,
                    cast_options,
                ),
                Int32 => cast_decimal_to_integer::<Decimal128Type, Int32Type>(
                    array,
                    10_i128,
                    *scale,
                    cast_options,
                ),
                Int64 => cast_decimal_to_integer::<Decimal128Type, Int64Type>(
                    array,
                    10_i128,
                    *scale,
                    cast_options,
                ),
                Float32 => {
                    cast_decimal_to_float::<Decimal128Type, Float32Type, _>(array, |x| {
                        (x as f64 / 10_f64.powi(*scale as i32)) as f32
                    })
                }
                Float64 => {
                    cast_decimal_to_float::<Decimal128Type, Float64Type, _>(array, |x| {
                        x as f64 / 10_f64.powi(*scale as i32)
                    })
                }
                Utf8 => value_to_string::<i32>(array),
                LargeUtf8 => value_to_string::<i64>(array),
                Null => Ok(new_null_array(to_type, array.len())),
                _ => Err(ArrowError::CastError(format!(
                    "Casting from {from_type:?} to {to_type:?} not supported"
                ))),
            }
        }
        (Decimal256(_, scale), _) => {
            // cast decimal to other type
            match to_type {
                UInt8 => cast_decimal_to_integer::<Decimal256Type, UInt8Type>(
                    array,
                    i256::from_i128(10_i128),
                    *scale,
                    cast_options,
                ),
                UInt16 => cast_decimal_to_integer::<Decimal256Type, UInt16Type>(
                    array,
                    i256::from_i128(10_i128),
                    *scale,
                    cast_options,
                ),
                UInt32 => cast_decimal_to_integer::<Decimal256Type, UInt32Type>(
                    array,
                    i256::from_i128(10_i128),
                    *scale,
                    cast_options,
                ),
                UInt64 => cast_decimal_to_integer::<Decimal256Type, UInt64Type>(
                    array,
                    i256::from_i128(10_i128),
                    *scale,
                    cast_options,
                ),
                Int8 => cast_decimal_to_integer::<Decimal256Type, Int8Type>(
                    array,
                    i256::from_i128(10_i128),
                    *scale,
                    cast_options,
                ),
                Int16 => cast_decimal_to_integer::<Decimal256Type, Int16Type>(
                    array,
                    i256::from_i128(10_i128),
                    *scale,
                    cast_options,
                ),
                Int32 => cast_decimal_to_integer::<Decimal256Type, Int32Type>(
                    array,
                    i256::from_i128(10_i128),
                    *scale,
                    cast_options,
                ),
                Int64 => cast_decimal_to_integer::<Decimal256Type, Int64Type>(
                    array,
                    i256::from_i128(10_i128),
                    *scale,
                    cast_options,
                ),
                Float32 => {
                    cast_decimal_to_float::<Decimal256Type, Float32Type, _>(array, |x| {
                        (x.to_f64().unwrap() / 10_f64.powi(*scale as i32)) as f32
                    })
                }
                Float64 => {
                    cast_decimal_to_float::<Decimal256Type, Float64Type, _>(array, |x| {
                        x.to_f64().unwrap() / 10_f64.powi(*scale as i32)
                    })
                }
                Utf8 => value_to_string::<i32>(array),
                LargeUtf8 => value_to_string::<i64>(array),
                Null => Ok(new_null_array(to_type, array.len())),
                _ => Err(ArrowError::CastError(format!(
                    "Casting from {from_type:?} to {to_type:?} not supported"
                ))),
            }
        }
        (_, Decimal128(precision, scale)) => {
            // cast data to decimal
            match from_type {
                UInt8 => cast_integer_to_decimal::<_, Decimal128Type, _>(
                    array.as_primitive::<UInt8Type>(),
                    *precision,
                    *scale,
                    10_i128,
                    cast_options,
                ),
                UInt16 => cast_integer_to_decimal::<_, Decimal128Type, _>(
                    array.as_primitive::<UInt16Type>(),
                    *precision,
                    *scale,
                    10_i128,
                    cast_options,
                ),
                UInt32 => cast_integer_to_decimal::<_, Decimal128Type, _>(
                    array.as_primitive::<UInt32Type>(),
                    *precision,
                    *scale,
                    10_i128,
                    cast_options,
                ),
                UInt64 => cast_integer_to_decimal::<_, Decimal128Type, _>(
                    array.as_primitive::<UInt64Type>(),
                    *precision,
                    *scale,
                    10_i128,
                    cast_options,
                ),
                Int8 => cast_integer_to_decimal::<_, Decimal128Type, _>(
                    array.as_primitive::<Int8Type>(),
                    *precision,
                    *scale,
                    10_i128,
                    cast_options,
                ),
                Int16 => cast_integer_to_decimal::<_, Decimal128Type, _>(
                    array.as_primitive::<Int16Type>(),
                    *precision,
                    *scale,
                    10_i128,
                    cast_options,
                ),
                Int32 => cast_integer_to_decimal::<_, Decimal128Type, _>(
                    array.as_primitive::<Int32Type>(),
                    *precision,
                    *scale,
                    10_i128,
                    cast_options,
                ),
                Int64 => cast_integer_to_decimal::<_, Decimal128Type, _>(
                    array.as_primitive::<Int64Type>(),
                    *precision,
                    *scale,
                    10_i128,
                    cast_options,
                ),
                Float32 => cast_floating_point_to_decimal128(
                    array.as_primitive::<Float32Type>(),
                    *precision,
                    *scale,
                    cast_options,
                ),
                Float64 => cast_floating_point_to_decimal128(
                    array.as_primitive::<Float64Type>(),
                    *precision,
                    *scale,
                    cast_options,
                ),
                Utf8 => cast_string_to_decimal::<Decimal128Type, i32>(
                    array,
                    *precision,
                    *scale,
                    cast_options,
                ),
                LargeUtf8 => cast_string_to_decimal::<Decimal128Type, i64>(
                    array,
                    *precision,
                    *scale,
                    cast_options,
                ),
                Null => Ok(new_null_array(to_type, array.len())),
                _ => Err(ArrowError::CastError(format!(
                    "Casting from {from_type:?} to {to_type:?} not supported"
                ))),
            }
        }
        (_, Decimal256(precision, scale)) => {
            // cast data to decimal
            match from_type {
                UInt8 => cast_integer_to_decimal::<_, Decimal256Type, _>(
                    array.as_primitive::<UInt8Type>(),
                    *precision,
                    *scale,
                    i256::from_i128(10_i128),
                    cast_options,
                ),
                UInt16 => cast_integer_to_decimal::<_, Decimal256Type, _>(
                    array.as_primitive::<UInt16Type>(),
                    *precision,
                    *scale,
                    i256::from_i128(10_i128),
                    cast_options,
                ),
                UInt32 => cast_integer_to_decimal::<_, Decimal256Type, _>(
                    array.as_primitive::<UInt32Type>(),
                    *precision,
                    *scale,
                    i256::from_i128(10_i128),
                    cast_options,
                ),
                UInt64 => cast_integer_to_decimal::<_, Decimal256Type, _>(
                    array.as_primitive::<UInt64Type>(),
                    *precision,
                    *scale,
                    i256::from_i128(10_i128),
                    cast_options,
                ),
                Int8 => cast_integer_to_decimal::<_, Decimal256Type, _>(
                    array.as_primitive::<Int8Type>(),
                    *precision,
                    *scale,
                    i256::from_i128(10_i128),
                    cast_options,
                ),
                Int16 => cast_integer_to_decimal::<_, Decimal256Type, _>(
                    array.as_primitive::<Int16Type>(),
                    *precision,
                    *scale,
                    i256::from_i128(10_i128),
                    cast_options,
                ),
                Int32 => cast_integer_to_decimal::<_, Decimal256Type, _>(
                    array.as_primitive::<Int32Type>(),
                    *precision,
                    *scale,
                    i256::from_i128(10_i128),
                    cast_options,
                ),
                Int64 => cast_integer_to_decimal::<_, Decimal256Type, _>(
                    array.as_primitive::<Int64Type>(),
                    *precision,
                    *scale,
                    i256::from_i128(10_i128),
                    cast_options,
                ),
                Float32 => cast_floating_point_to_decimal256(
                    array.as_primitive::<Float32Type>(),
                    *precision,
                    *scale,
                    cast_options,
                ),
                Float64 => cast_floating_point_to_decimal256(
                    array.as_primitive::<Float64Type>(),
                    *precision,
                    *scale,
                    cast_options,
                ),
                Utf8 => cast_string_to_decimal::<Decimal256Type, i32>(
                    array,
                    *precision,
                    *scale,
                    cast_options,
                ),
                LargeUtf8 => cast_string_to_decimal::<Decimal256Type, i64>(
                    array,
                    *precision,
                    *scale,
                    cast_options,
                ),
                Null => Ok(new_null_array(to_type, array.len())),
                _ => Err(ArrowError::CastError(format!(
                    "Casting from {from_type:?} to {to_type:?} not supported"
                ))),
            }
        }
        (Struct(_), _) => Err(ArrowError::CastError(
            "Cannot cast from struct to other types".to_string(),
        )),
        (_, Struct(_)) => Err(ArrowError::CastError(
            "Cannot cast to struct from other types".to_string(),
        )),
        (_, Boolean) => match from_type {
            UInt8 => cast_numeric_to_bool::<UInt8Type>(array),
            UInt16 => cast_numeric_to_bool::<UInt16Type>(array),
            UInt32 => cast_numeric_to_bool::<UInt32Type>(array),
            UInt64 => cast_numeric_to_bool::<UInt64Type>(array),
            Int8 => cast_numeric_to_bool::<Int8Type>(array),
            Int16 => cast_numeric_to_bool::<Int16Type>(array),
            Int32 => cast_numeric_to_bool::<Int32Type>(array),
            Int64 => cast_numeric_to_bool::<Int64Type>(array),
            Float16 => cast_numeric_to_bool::<Float16Type>(array),
            Float32 => cast_numeric_to_bool::<Float32Type>(array),
            Float64 => cast_numeric_to_bool::<Float64Type>(array),
            Utf8 => cast_utf8_to_boolean::<i32>(array, cast_options),
            LargeUtf8 => cast_utf8_to_boolean::<i64>(array, cast_options),
            _ => Err(ArrowError::CastError(format!(
                "Casting from {from_type:?} to {to_type:?} not supported",
            ))),
        },
        (Boolean, _) => match to_type {
            UInt8 => cast_bool_to_numeric::<UInt8Type>(array, cast_options),
            UInt16 => cast_bool_to_numeric::<UInt16Type>(array, cast_options),
            UInt32 => cast_bool_to_numeric::<UInt32Type>(array, cast_options),
            UInt64 => cast_bool_to_numeric::<UInt64Type>(array, cast_options),
            Int8 => cast_bool_to_numeric::<Int8Type>(array, cast_options),
            Int16 => cast_bool_to_numeric::<Int16Type>(array, cast_options),
            Int32 => cast_bool_to_numeric::<Int32Type>(array, cast_options),
            Int64 => cast_bool_to_numeric::<Int64Type>(array, cast_options),
            Float16 => cast_bool_to_numeric::<Float16Type>(array, cast_options),
            Float32 => cast_bool_to_numeric::<Float32Type>(array, cast_options),
            Float64 => cast_bool_to_numeric::<Float64Type>(array, cast_options),
            Utf8 => {
                let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                Ok(Arc::new(
                    array
                        .iter()
                        .map(|value| value.map(|value| if value { "1" } else { "0" }))
                        .collect::<StringArray>(),
                ))
            }
            LargeUtf8 => {
                let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                Ok(Arc::new(
                    array
                        .iter()
                        .map(|value| value.map(|value| if value { "1" } else { "0" }))
                        .collect::<LargeStringArray>(),
                ))
            }
            _ => Err(ArrowError::CastError(format!(
                "Casting from {from_type:?} to {to_type:?} not supported",
            ))),
        },
        (Utf8, _) => match to_type {
            UInt8 => cast_string_to_numeric::<UInt8Type, i32>(array, cast_options),
            UInt16 => cast_string_to_numeric::<UInt16Type, i32>(array, cast_options),
            UInt32 => cast_string_to_numeric::<UInt32Type, i32>(array, cast_options),
            UInt64 => cast_string_to_numeric::<UInt64Type, i32>(array, cast_options),
            Int8 => cast_string_to_numeric::<Int8Type, i32>(array, cast_options),
            Int16 => cast_string_to_numeric::<Int16Type, i32>(array, cast_options),
            Int32 => cast_string_to_numeric::<Int32Type, i32>(array, cast_options),
            Int64 => cast_string_to_numeric::<Int64Type, i32>(array, cast_options),
            Float32 => cast_string_to_numeric::<Float32Type, i32>(array, cast_options),
            Float64 => cast_string_to_numeric::<Float64Type, i32>(array, cast_options),
            Date32 => cast_string_to_date32::<i32>(array, cast_options),
            Date64 => cast_string_to_date64::<i32>(array, cast_options),
            Binary => Ok(Arc::new(BinaryArray::from(array.as_string::<i32>().clone()))),
            LargeBinary => {
                let binary = BinaryArray::from(array.as_string::<i32>().clone());
                cast_byte_container::<BinaryType, LargeBinaryType>(&binary)
            }
            LargeUtf8 => cast_byte_container::<Utf8Type, LargeUtf8Type>(array),
            Time32(TimeUnit::Second) => {
                cast_string_to_time32second::<i32>(array, cast_options)
            }
            Time32(TimeUnit::Millisecond) => {
                cast_string_to_time32millisecond::<i32>(array, cast_options)
            }
            Time64(TimeUnit::Microsecond) => {
                cast_string_to_time64microsecond::<i32>(array, cast_options)
            }
            Time64(TimeUnit::Nanosecond) => {
                cast_string_to_time64nanosecond::<i32>(array, cast_options)
            }
            Timestamp(TimeUnit::Second, to_tz) => {
                cast_string_to_timestamp::<i32, TimestampSecondType>(array, to_tz, cast_options)
            }
            Timestamp(TimeUnit::Millisecond, to_tz) => {
                cast_string_to_timestamp::<i32, TimestampMillisecondType>(array, to_tz, cast_options)
            }
            Timestamp(TimeUnit::Microsecond, to_tz) => {
                cast_string_to_timestamp::<i32, TimestampMicrosecondType>(array, to_tz, cast_options)
            }
            Timestamp(TimeUnit::Nanosecond, to_tz) => {
                cast_string_to_timestamp::<i32, TimestampNanosecondType>(array, to_tz, cast_options)
            }
            Interval(IntervalUnit::YearMonth) => {
                cast_string_to_year_month_interval::<i32>(array, cast_options)
            }
            Interval(IntervalUnit::DayTime) => {
                cast_string_to_day_time_interval::<i32>(array, cast_options)
            }
            Interval(IntervalUnit::MonthDayNano) => {
                cast_string_to_month_day_nano_interval::<i32>(array, cast_options)
            }
            _ => Err(ArrowError::CastError(format!(
                "Casting from {from_type:?} to {to_type:?} not supported",
            ))),
        },
        (LargeUtf8, _) => match to_type {
            UInt8 => cast_string_to_numeric::<UInt8Type, i64>(array, cast_options),
            UInt16 => cast_string_to_numeric::<UInt16Type, i64>(array, cast_options),
            UInt32 => cast_string_to_numeric::<UInt32Type, i64>(array, cast_options),
            UInt64 => cast_string_to_numeric::<UInt64Type, i64>(array, cast_options),
            Int8 => cast_string_to_numeric::<Int8Type, i64>(array, cast_options),
            Int16 => cast_string_to_numeric::<Int16Type, i64>(array, cast_options),
            Int32 => cast_string_to_numeric::<Int32Type, i64>(array, cast_options),
            Int64 => cast_string_to_numeric::<Int64Type, i64>(array, cast_options),
            Float32 => cast_string_to_numeric::<Float32Type, i64>(array, cast_options),
            Float64 => cast_string_to_numeric::<Float64Type, i64>(array, cast_options),
            Date32 => cast_string_to_date32::<i64>(array, cast_options),
            Date64 => cast_string_to_date64::<i64>(array, cast_options),
            Utf8 => cast_byte_container::<LargeUtf8Type, Utf8Type>(array),
            Binary => {
                let large_binary =
                    LargeBinaryArray::from(array.as_string::<i64>().clone());
                cast_byte_container::<LargeBinaryType, BinaryType>(&large_binary)
            }
            LargeBinary => Ok(Arc::new(LargeBinaryArray::from(
                array.as_string::<i64>().clone(),
            ))),
            Time32(TimeUnit::Second) => {
                cast_string_to_time32second::<i64>(array, cast_options)
            }
            Time32(TimeUnit::Millisecond) => {
                cast_string_to_time32millisecond::<i64>(array, cast_options)
            }
            Time64(TimeUnit::Microsecond) => {
                cast_string_to_time64microsecond::<i64>(array, cast_options)
            }
            Time64(TimeUnit::Nanosecond) => {
                cast_string_to_time64nanosecond::<i64>(array, cast_options)
            }
            Timestamp(TimeUnit::Second, to_tz) => {
                cast_string_to_timestamp::<i64, TimestampSecondType>(array, to_tz, cast_options)
            }
            Timestamp(TimeUnit::Millisecond, to_tz) => {
                cast_string_to_timestamp::<i64, TimestampMillisecondType>(array, to_tz, cast_options)
            }
            Timestamp(TimeUnit::Microsecond, to_tz) => {
                cast_string_to_timestamp::<i64, TimestampMicrosecondType>(array, to_tz, cast_options)
            }
            Timestamp(TimeUnit::Nanosecond, to_tz) => {
                cast_string_to_timestamp::<i64, TimestampNanosecondType>(array, to_tz, cast_options)
            }
            Interval(IntervalUnit::YearMonth) => {
                cast_string_to_year_month_interval::<i64>(array, cast_options)
            }
            Interval(IntervalUnit::DayTime) => {
                cast_string_to_day_time_interval::<i64>(array, cast_options)
            }
            Interval(IntervalUnit::MonthDayNano) => {
                cast_string_to_month_day_nano_interval::<i64>(array, cast_options)
            }
            _ => Err(ArrowError::CastError(format!(
                "Casting from {from_type:?} to {to_type:?} not supported",
            ))),
        },
        (Binary, _) => match to_type {
            Utf8 => cast_binary_to_string::<i32>(array, cast_options),
            LargeUtf8 => {
                let array = cast_binary_to_string::<i32>(array, cast_options)?;
                cast_byte_container::<Utf8Type, LargeUtf8Type>(array.as_ref())
            }
            LargeBinary => {
                cast_byte_container::<BinaryType, LargeBinaryType>(array)
            }
            FixedSizeBinary(size) => {
                cast_binary_to_fixed_size_binary::<i32>(array, *size, cast_options)
            }
            _ => Err(ArrowError::CastError(format!(
                "Casting from {from_type:?} to {to_type:?} not supported",
            ))),
        },
        (LargeBinary, _) => match to_type {
            Utf8 => {
                let array = cast_binary_to_string::<i64>(array, cast_options)?;
                cast_byte_container::<LargeUtf8Type, Utf8Type>(array.as_ref())
            }
            LargeUtf8 => cast_binary_to_string::<i64>(array, cast_options),
            Binary => cast_byte_container::<LargeBinaryType, BinaryType>(array),
            FixedSizeBinary(size) => {
                cast_binary_to_fixed_size_binary::<i64>(array, *size, cast_options)
            }
            _ => Err(ArrowError::CastError(format!(
                "Casting from {from_type:?} to {to_type:?} not supported",
            ))),
        },
        (FixedSizeBinary(size), _) => match to_type {
            Binary => cast_fixed_size_binary_to_binary::<i32>(array, *size),
            LargeBinary =>
                cast_fixed_size_binary_to_binary::<i64>(array, *size),
            _ => Err(ArrowError::CastError(format!(
                "Casting from {from_type:?} to {to_type:?} not supported",
            ))),
        },
        (from_type, LargeUtf8) if from_type.is_primitive() => value_to_string::<i64>(array),
        (from_type, Utf8) if from_type.is_primitive() => value_to_string::<i32>(array),
        // start numeric casts
        (UInt8, UInt16) => {
            cast_numeric_arrays::<UInt8Type, UInt16Type>(array, cast_options)
        }
        (UInt8, UInt32) => {
            cast_numeric_arrays::<UInt8Type, UInt32Type>(array, cast_options)
        }
        (UInt8, UInt64) => {
            cast_numeric_arrays::<UInt8Type, UInt64Type>(array, cast_options)
        }
        (UInt8, Int8) => cast_numeric_arrays::<UInt8Type, Int8Type>(array, cast_options),
        (UInt8, Int16) => {
            cast_numeric_arrays::<UInt8Type, Int16Type>(array, cast_options)
        }
        (UInt8, Int32) => {
            cast_numeric_arrays::<UInt8Type, Int32Type>(array, cast_options)
        }
        (UInt8, Int64) => {
            cast_numeric_arrays::<UInt8Type, Int64Type>(array, cast_options)
        }
        (UInt8, Float32) => {
            cast_numeric_arrays::<UInt8Type, Float32Type>(array, cast_options)
        }
        (UInt8, Float64) => {
            cast_numeric_arrays::<UInt8Type, Float64Type>(array, cast_options)
        }

        (UInt16, UInt8) => {
            cast_numeric_arrays::<UInt16Type, UInt8Type>(array, cast_options)
        }
        (UInt16, UInt32) => {
            cast_numeric_arrays::<UInt16Type, UInt32Type>(array, cast_options)
        }
        (UInt16, UInt64) => {
            cast_numeric_arrays::<UInt16Type, UInt64Type>(array, cast_options)
        }
        (UInt16, Int8) => {
            cast_numeric_arrays::<UInt16Type, Int8Type>(array, cast_options)
        }
        (UInt16, Int16) => {
            cast_numeric_arrays::<UInt16Type, Int16Type>(array, cast_options)
        }
        (UInt16, Int32) => {
            cast_numeric_arrays::<UInt16Type, Int32Type>(array, cast_options)
        }
        (UInt16, Int64) => {
            cast_numeric_arrays::<UInt16Type, Int64Type>(array, cast_options)
        }
        (UInt16, Float32) => {
            cast_numeric_arrays::<UInt16Type, Float32Type>(array, cast_options)
        }
        (UInt16, Float64) => {
            cast_numeric_arrays::<UInt16Type, Float64Type>(array, cast_options)
        }

        (UInt32, UInt8) => {
            cast_numeric_arrays::<UInt32Type, UInt8Type>(array, cast_options)
        }
        (UInt32, UInt16) => {
            cast_numeric_arrays::<UInt32Type, UInt16Type>(array, cast_options)
        }
        (UInt32, UInt64) => {
            cast_numeric_arrays::<UInt32Type, UInt64Type>(array, cast_options)
        }
        (UInt32, Int8) => {
            cast_numeric_arrays::<UInt32Type, Int8Type>(array, cast_options)
        }
        (UInt32, Int16) => {
            cast_numeric_arrays::<UInt32Type, Int16Type>(array, cast_options)
        }
        (UInt32, Int32) => {
            cast_numeric_arrays::<UInt32Type, Int32Type>(array, cast_options)
        }
        (UInt32, Int64) => {
            cast_numeric_arrays::<UInt32Type, Int64Type>(array, cast_options)
        }
        (UInt32, Float32) => {
            cast_numeric_arrays::<UInt32Type, Float32Type>(array, cast_options)
        }
        (UInt32, Float64) => {
            cast_numeric_arrays::<UInt32Type, Float64Type>(array, cast_options)
        }

        (UInt64, UInt8) => {
            cast_numeric_arrays::<UInt64Type, UInt8Type>(array, cast_options)
        }
        (UInt64, UInt16) => {
            cast_numeric_arrays::<UInt64Type, UInt16Type>(array, cast_options)
        }
        (UInt64, UInt32) => {
            cast_numeric_arrays::<UInt64Type, UInt32Type>(array, cast_options)
        }
        (UInt64, Int8) => {
            cast_numeric_arrays::<UInt64Type, Int8Type>(array, cast_options)
        }
        (UInt64, Int16) => {
            cast_numeric_arrays::<UInt64Type, Int16Type>(array, cast_options)
        }
        (UInt64, Int32) => {
            cast_numeric_arrays::<UInt64Type, Int32Type>(array, cast_options)
        }
        (UInt64, Int64) => {
            cast_numeric_arrays::<UInt64Type, Int64Type>(array, cast_options)
        }
        (UInt64, Float32) => {
            cast_numeric_arrays::<UInt64Type, Float32Type>(array, cast_options)
        }
        (UInt64, Float64) => {
            cast_numeric_arrays::<UInt64Type, Float64Type>(array, cast_options)
        }

        (Int8, UInt8) => cast_numeric_arrays::<Int8Type, UInt8Type>(array, cast_options),
        (Int8, UInt16) => {
            cast_numeric_arrays::<Int8Type, UInt16Type>(array, cast_options)
        }
        (Int8, UInt32) => {
            cast_numeric_arrays::<Int8Type, UInt32Type>(array, cast_options)
        }
        (Int8, UInt64) => {
            cast_numeric_arrays::<Int8Type, UInt64Type>(array, cast_options)
        }
        (Int8, Int16) => cast_numeric_arrays::<Int8Type, Int16Type>(array, cast_options),
        (Int8, Int32) => cast_numeric_arrays::<Int8Type, Int32Type>(array, cast_options),
        (Int8, Int64) => cast_numeric_arrays::<Int8Type, Int64Type>(array, cast_options),
        (Int8, Float32) => {
            cast_numeric_arrays::<Int8Type, Float32Type>(array, cast_options)
        }
        (Int8, Float64) => {
            cast_numeric_arrays::<Int8Type, Float64Type>(array, cast_options)
        }

        (Int16, UInt8) => {
            cast_numeric_arrays::<Int16Type, UInt8Type>(array, cast_options)
        }
        (Int16, UInt16) => {
            cast_numeric_arrays::<Int16Type, UInt16Type>(array, cast_options)
        }
        (Int16, UInt32) => {
            cast_numeric_arrays::<Int16Type, UInt32Type>(array, cast_options)
        }
        (Int16, UInt64) => {
            cast_numeric_arrays::<Int16Type, UInt64Type>(array, cast_options)
        }
        (Int16, Int8) => cast_numeric_arrays::<Int16Type, Int8Type>(array, cast_options),
        (Int16, Int32) => {
            cast_numeric_arrays::<Int16Type, Int32Type>(array, cast_options)
        }
        (Int16, Int64) => {
            cast_numeric_arrays::<Int16Type, Int64Type>(array, cast_options)
        }
        (Int16, Float32) => {
            cast_numeric_arrays::<Int16Type, Float32Type>(array, cast_options)
        }
        (Int16, Float64) => {
            cast_numeric_arrays::<Int16Type, Float64Type>(array, cast_options)
        }

        (Int32, UInt8) => {
            cast_numeric_arrays::<Int32Type, UInt8Type>(array, cast_options)
        }
        (Int32, UInt16) => {
            cast_numeric_arrays::<Int32Type, UInt16Type>(array, cast_options)
        }
        (Int32, UInt32) => {
            cast_numeric_arrays::<Int32Type, UInt32Type>(array, cast_options)
        }
        (Int32, UInt64) => {
            cast_numeric_arrays::<Int32Type, UInt64Type>(array, cast_options)
        }
        (Int32, Int8) => cast_numeric_arrays::<Int32Type, Int8Type>(array, cast_options),
        (Int32, Int16) => {
            cast_numeric_arrays::<Int32Type, Int16Type>(array, cast_options)
        }
        (Int32, Int64) => {
            cast_numeric_arrays::<Int32Type, Int64Type>(array, cast_options)
        }
        (Int32, Float32) => {
            cast_numeric_arrays::<Int32Type, Float32Type>(array, cast_options)
        }
        (Int32, Float64) => {
            cast_numeric_arrays::<Int32Type, Float64Type>(array, cast_options)
        }

        (Int64, UInt8) => {
            cast_numeric_arrays::<Int64Type, UInt8Type>(array, cast_options)
        }
        (Int64, UInt16) => {
            cast_numeric_arrays::<Int64Type, UInt16Type>(array, cast_options)
        }
        (Int64, UInt32) => {
            cast_numeric_arrays::<Int64Type, UInt32Type>(array, cast_options)
        }
        (Int64, UInt64) => {
            cast_numeric_arrays::<Int64Type, UInt64Type>(array, cast_options)
        }
        (Int64, Int8) => cast_numeric_arrays::<Int64Type, Int8Type>(array, cast_options),
        (Int64, Int16) => {
            cast_numeric_arrays::<Int64Type, Int16Type>(array, cast_options)
        }
        (Int64, Int32) => {
            cast_numeric_arrays::<Int64Type, Int32Type>(array, cast_options)
        }
        (Int64, Float32) => {
            cast_numeric_arrays::<Int64Type, Float32Type>(array, cast_options)
        }
        (Int64, Float64) => {
            cast_numeric_arrays::<Int64Type, Float64Type>(array, cast_options)
        }

        (Float32, UInt8) => {
            cast_numeric_arrays::<Float32Type, UInt8Type>(array, cast_options)
        }
        (Float32, UInt16) => {
            cast_numeric_arrays::<Float32Type, UInt16Type>(array, cast_options)
        }
        (Float32, UInt32) => {
            cast_numeric_arrays::<Float32Type, UInt32Type>(array, cast_options)
        }
        (Float32, UInt64) => {
            cast_numeric_arrays::<Float32Type, UInt64Type>(array, cast_options)
        }
        (Float32, Int8) => {
            cast_numeric_arrays::<Float32Type, Int8Type>(array, cast_options)
        }
        (Float32, Int16) => {
            cast_numeric_arrays::<Float32Type, Int16Type>(array, cast_options)
        }
        (Float32, Int32) => {
            cast_numeric_arrays::<Float32Type, Int32Type>(array, cast_options)
        }
        (Float32, Int64) => {
            cast_numeric_arrays::<Float32Type, Int64Type>(array, cast_options)
        }
        (Float32, Float64) => {
            cast_numeric_arrays::<Float32Type, Float64Type>(array, cast_options)
        }

        (Float64, UInt8) => {
            cast_numeric_arrays::<Float64Type, UInt8Type>(array, cast_options)
        }
        (Float64, UInt16) => {
            cast_numeric_arrays::<Float64Type, UInt16Type>(array, cast_options)
        }
        (Float64, UInt32) => {
            cast_numeric_arrays::<Float64Type, UInt32Type>(array, cast_options)
        }
        (Float64, UInt64) => {
            cast_numeric_arrays::<Float64Type, UInt64Type>(array, cast_options)
        }
        (Float64, Int8) => {
            cast_numeric_arrays::<Float64Type, Int8Type>(array, cast_options)
        }
        (Float64, Int16) => {
            cast_numeric_arrays::<Float64Type, Int16Type>(array, cast_options)
        }
        (Float64, Int32) => {
            cast_numeric_arrays::<Float64Type, Int32Type>(array, cast_options)
        }
        (Float64, Int64) => {
            cast_numeric_arrays::<Float64Type, Int64Type>(array, cast_options)
        }
        (Float64, Float32) => {
            cast_numeric_arrays::<Float64Type, Float32Type>(array, cast_options)
        }
        // end numeric casts

        // temporal casts
        (Int32, Date32) => cast_reinterpret_arrays::<Int32Type, Date32Type>(array),
        (Int32, Date64) => cast_with_options(
            &cast_with_options(array, &Date32, cast_options)?,
            &Date64,
            cast_options,
        ),
        (Int32, Time32(TimeUnit::Second)) => {
            cast_reinterpret_arrays::<Int32Type, Time32SecondType>(array)
        }
        (Int32, Time32(TimeUnit::Millisecond)) => {
            cast_reinterpret_arrays::<Int32Type, Time32MillisecondType>(array)
        }
        // No support for microsecond/nanosecond with i32
        (Date32, Int32) => cast_reinterpret_arrays::<Date32Type, Int32Type>(array),
        (Date32, Int64) => cast_with_options(
            &cast_with_options(array, &Int32, cast_options)?,
            &Int64,
            cast_options,
        ),
        (Time32(TimeUnit::Second), Int32) => {
            cast_reinterpret_arrays::<Time32SecondType, Int32Type>(array)
        }
        (Time32(TimeUnit::Millisecond), Int32) => {
            cast_reinterpret_arrays::<Time32MillisecondType, Int32Type>(array)
        }
        (Int64, Date64) => cast_reinterpret_arrays::<Int64Type, Date64Type>(array),
        (Int64, Date32) => cast_with_options(
            &cast_with_options(array, &Int32, cast_options)?,
            &Date32,
            cast_options,
        ),
        // No support for second/milliseconds with i64
        (Int64, Time64(TimeUnit::Microsecond)) => {
            cast_reinterpret_arrays::<Int64Type, Time64MicrosecondType>(array)
        }
        (Int64, Time64(TimeUnit::Nanosecond)) => {
            cast_reinterpret_arrays::<Int64Type, Time64NanosecondType>(array)
        }

        (Date64, Int64) => cast_reinterpret_arrays::<Date64Type, Int64Type>(array),
        (Date64, Int32) => cast_with_options(
            &cast_with_options(array, &Int64, cast_options)?,
            &Int32,
            cast_options,
        ),
        (Time64(TimeUnit::Microsecond), Int64) => {
            cast_reinterpret_arrays::<Time64MicrosecondType, Int64Type>(array)
        }
        (Time64(TimeUnit::Nanosecond), Int64) => {
            cast_reinterpret_arrays::<Time64NanosecondType, Int64Type>(array)
        }
        (Date32, Date64) => Ok(Arc::new(
            array.as_primitive::<Date32Type>()
                .unary::<_, Date64Type>(|x| x as i64 * MILLISECONDS_IN_DAY),
        )),
        (Date64, Date32) => Ok(Arc::new(
            array.as_primitive::<Date64Type>()
                .unary::<_, Date32Type>(|x| (x / MILLISECONDS_IN_DAY) as i32),
        )),

        (Time32(TimeUnit::Second), Time32(TimeUnit::Millisecond)) => Ok(Arc::new(
            array.as_primitive::<Time32SecondType>()
                .unary::<_, Time32MillisecondType>(|x| x * MILLISECONDS as i32),
        )),
        (Time32(TimeUnit::Second), Time64(TimeUnit::Microsecond)) => Ok(Arc::new(
            array.as_primitive::<Time32SecondType>()
                .unary::<_, Time64MicrosecondType>(|x| x as i64 * MICROSECONDS),
        )),
        (Time32(TimeUnit::Second), Time64(TimeUnit::Nanosecond)) => Ok(Arc::new(
            array.as_primitive::<Time32SecondType>()
                .unary::<_, Time64NanosecondType>(|x| x as i64 * NANOSECONDS),
        )),

        (Time32(TimeUnit::Millisecond), Time32(TimeUnit::Second)) => Ok(Arc::new(
            array.as_primitive::<Time32MillisecondType>()
                .unary::<_, Time32SecondType>(|x| x / MILLISECONDS as i32),
        )),
        (Time32(TimeUnit::Millisecond), Time64(TimeUnit::Microsecond)) => Ok(Arc::new(
            array.as_primitive::<Time32MillisecondType>()
                .unary::<_, Time64MicrosecondType>(|x| {
                    x as i64 * (MICROSECONDS / MILLISECONDS)
                }),
        )),
        (Time32(TimeUnit::Millisecond), Time64(TimeUnit::Nanosecond)) => Ok(Arc::new(
            array.as_primitive::<Time32MillisecondType>()
                .unary::<_, Time64NanosecondType>(|x| {
                    x as i64 * (MICROSECONDS / NANOSECONDS)
                }),
        )),

        (Time64(TimeUnit::Microsecond), Time32(TimeUnit::Second)) => Ok(Arc::new(
            array.as_primitive::<Time64MicrosecondType>()
                .unary::<_, Time32SecondType>(|x| (x / MICROSECONDS) as i32),
        )),
        (Time64(TimeUnit::Microsecond), Time32(TimeUnit::Millisecond)) => Ok(Arc::new(
            array.as_primitive::<Time64MicrosecondType>()
                .unary::<_, Time32MillisecondType>(|x| {
                    (x / (MICROSECONDS / MILLISECONDS)) as i32
                }),
        )),
        (Time64(TimeUnit::Microsecond), Time64(TimeUnit::Nanosecond)) => Ok(Arc::new(
            array.as_primitive::<Time64MicrosecondType>()
                .unary::<_, Time64NanosecondType>(|x| x * (NANOSECONDS / MICROSECONDS)),
        )),

        (Time64(TimeUnit::Nanosecond), Time32(TimeUnit::Second)) => Ok(Arc::new(
            array.as_primitive::<Time64NanosecondType>()
                .unary::<_, Time32SecondType>(|x| (x / NANOSECONDS) as i32),
        )),
        (Time64(TimeUnit::Nanosecond), Time32(TimeUnit::Millisecond)) => Ok(Arc::new(
            array.as_primitive::<Time64NanosecondType>()
                .unary::<_, Time32MillisecondType>(|x| {
                    (x / (NANOSECONDS / MILLISECONDS)) as i32
                }),
        )),
        (Time64(TimeUnit::Nanosecond), Time64(TimeUnit::Microsecond)) => Ok(Arc::new(
            array.as_primitive::<Time64NanosecondType>()
                .unary::<_, Time64MicrosecondType>(|x| x / (NANOSECONDS / MICROSECONDS)),
        )),

        (Timestamp(TimeUnit::Second, _), Int64) => {
            cast_reinterpret_arrays::<TimestampSecondType, Int64Type>(array)
        }
        (Timestamp(TimeUnit::Millisecond, _), Int64) => {
            cast_reinterpret_arrays::<TimestampMillisecondType, Int64Type>(array)
        }
        (Timestamp(TimeUnit::Microsecond, _), Int64) => {
            cast_reinterpret_arrays::<TimestampMicrosecondType, Int64Type>(array)
        }
        (Timestamp(TimeUnit::Nanosecond, _), Int64) => {
            cast_reinterpret_arrays::<TimestampNanosecondType, Int64Type>(array)
        }

        (Int64, Timestamp(unit, tz)) => Ok(make_timestamp_array(
            array.as_primitive(),
            unit.clone(),
            tz.clone(),
        )),

        (Timestamp(from_unit, _), Timestamp(to_unit, to_tz)) => {
            let array = cast_with_options(array, &Int64, cast_options)?;
            let time_array = array.as_primitive::<Int64Type>();
            let from_size = time_unit_multiple(from_unit);
            let to_size = time_unit_multiple(to_unit);
            // we either divide or multiply, depending on size of each unit
            // units are never the same when the types are the same
            let converted = match from_size.cmp(&to_size) {
                Ordering::Greater => {
                    let divisor = from_size / to_size;
                    time_array.unary::<_, Int64Type>(|o| o / divisor)
                }
                Ordering::Equal => time_array.clone(),
                Ordering::Less => {
                    let mul = to_size / from_size;
                    if cast_options.safe {
                        time_array.unary_opt::<_, Int64Type>(|o| o.checked_mul(mul))
                    } else {
                        time_array.try_unary::<_, Int64Type, _>(|o| o.mul_checked(mul))?
                    }
                }
            };
            Ok(make_timestamp_array(
                &converted,
                to_unit.clone(),
                to_tz.clone(),
            ))
        }
        (Timestamp(from_unit, _), Date32) => {
            let array = cast_with_options(array, &Int64, cast_options)?;
            let time_array = array.as_primitive::<Int64Type>();
            let from_size = time_unit_multiple(from_unit) * SECONDS_IN_DAY;

            let mut b = Date32Builder::with_capacity(array.len());

            for i in 0..array.len() {
                if time_array.is_null(i) {
                    b.append_null();
                } else {
                    b.append_value((time_array.value(i) / from_size) as i32);
                }
            }

            Ok(Arc::new(b.finish()) as ArrayRef)
        }
        (Timestamp(TimeUnit::Second, _), Date64) => Ok(Arc::new(
            match cast_options.safe {
                true => {
                    // change error to None
                    array.as_primitive::<TimestampSecondType>()
                        .unary_opt::<_, Date64Type>(|x| {
                            x.checked_mul(MILLISECONDS)
                        })
                }
                false => {
                    array.as_primitive::<TimestampSecondType>().try_unary::<_, Date64Type, _>(
                        |x| {
                            x.mul_checked(MILLISECONDS)
                        },
                    )?
                }
            },
        )),
        (Timestamp(TimeUnit::Millisecond, _), Date64) => {
            cast_reinterpret_arrays::<TimestampMillisecondType, Date64Type>(array)
        }
        (Timestamp(TimeUnit::Microsecond, _), Date64) => Ok(Arc::new(
            array.as_primitive::<TimestampMicrosecondType>()
                .unary::<_, Date64Type>(|x| x / (MICROSECONDS / MILLISECONDS)),
        )),
        (Timestamp(TimeUnit::Nanosecond, _), Date64) => Ok(Arc::new(
            array.as_primitive::<TimestampNanosecondType>()
                .unary::<_, Date64Type>(|x| x / (NANOSECONDS / MILLISECONDS)),
        )),
        (Timestamp(TimeUnit::Second, tz), Time64(TimeUnit::Microsecond)) => {
            let tz = tz.as_ref().map(|tz| tz.parse()).transpose()?;
            Ok(Arc::new(
                array.as_primitive::<TimestampSecondType>()
                    .try_unary::<_, Time64MicrosecondType, ArrowError>(|x| {
                        Ok(time_to_time64us(as_time_res_with_timezone::<
                            TimestampSecondType,
                        >(x, tz)?))
                    })?,
            ))
        }
        (Timestamp(TimeUnit::Second, tz), Time64(TimeUnit::Nanosecond)) => {
            let tz = tz.as_ref().map(|tz| tz.parse()).transpose()?;
            Ok(Arc::new(
                array.as_primitive::<TimestampSecondType>()
                    .try_unary::<_, Time64NanosecondType, ArrowError>(|x| {
                        Ok(time_to_time64ns(as_time_res_with_timezone::<
                            TimestampSecondType,
                        >(x, tz)?))
                    })?,
            ))
        }
        (Timestamp(TimeUnit::Millisecond, tz), Time64(TimeUnit::Microsecond)) => {
            let tz = tz.as_ref().map(|tz| tz.parse()).transpose()?;
            Ok(Arc::new(
                array.as_primitive::<TimestampMillisecondType>()
                    .try_unary::<_, Time64MicrosecondType, ArrowError>(|x| {
                        Ok(time_to_time64us(as_time_res_with_timezone::<
                            TimestampMillisecondType,
                        >(x, tz)?))
                    })?,
            ))
        }
        (Timestamp(TimeUnit::Millisecond, tz), Time64(TimeUnit::Nanosecond)) => {
            let tz = tz.as_ref().map(|tz| tz.parse()).transpose()?;
            Ok(Arc::new(
                array.as_primitive::<TimestampMillisecondType>()
                    .try_unary::<_, Time64NanosecondType, ArrowError>(|x| {
                        Ok(time_to_time64ns(as_time_res_with_timezone::<
                            TimestampMillisecondType,
                        >(x, tz)?))
                    })?,
            ))
        }
        (Timestamp(TimeUnit::Microsecond, tz), Time64(TimeUnit::Microsecond)) => {
            let tz = tz.as_ref().map(|tz| tz.parse()).transpose()?;
            Ok(Arc::new(
                array.as_primitive::<TimestampMicrosecondType>()
                    .try_unary::<_, Time64MicrosecondType, ArrowError>(|x| {
                        Ok(time_to_time64us(as_time_res_with_timezone::<
                            TimestampMicrosecondType,
                        >(x, tz)?))
                    })?,
            ))
        }
        (Timestamp(TimeUnit::Microsecond, tz), Time64(TimeUnit::Nanosecond)) => {
            let tz = tz.as_ref().map(|tz| tz.parse()).transpose()?;
            Ok(Arc::new(
                array.as_primitive::<TimestampMicrosecondType>()
                    .try_unary::<_, Time64NanosecondType, ArrowError>(|x| {
                        Ok(time_to_time64ns(as_time_res_with_timezone::<
                            TimestampMicrosecondType,
                        >(x, tz)?))
                    })?,
            ))
        }
        (Timestamp(TimeUnit::Nanosecond, tz), Time64(TimeUnit::Microsecond)) => {
            let tz = tz.as_ref().map(|tz| tz.parse()).transpose()?;
            Ok(Arc::new(
                array.as_primitive::<TimestampNanosecondType>()
                    .try_unary::<_, Time64MicrosecondType, ArrowError>(|x| {
                        Ok(time_to_time64us(as_time_res_with_timezone::<
                            TimestampNanosecondType,
                        >(x, tz)?))
                    })?,
            ))
        }
        (Timestamp(TimeUnit::Nanosecond, tz), Time64(TimeUnit::Nanosecond)) => {
            let tz = tz.as_ref().map(|tz| tz.parse()).transpose()?;
            Ok(Arc::new(
                array.as_primitive::<TimestampNanosecondType>()
                    .try_unary::<_, Time64NanosecondType, ArrowError>(|x| {
                        Ok(time_to_time64ns(as_time_res_with_timezone::<
                            TimestampNanosecondType,
                        >(x, tz)?))
                    })?,
            ))
        }
        (Timestamp(TimeUnit::Second, tz), Time32(TimeUnit::Second)) => {
            let tz = tz.as_ref().map(|tz| tz.parse()).transpose()?;
            Ok(Arc::new(
                array.as_primitive::<TimestampSecondType>()
                    .try_unary::<_, Time32SecondType, ArrowError>(|x| {
                        Ok(time_to_time32s(as_time_res_with_timezone::<
                            TimestampSecondType,
                        >(x, tz)?))
                    })?,
            ))
        }
        (Timestamp(TimeUnit::Second, tz), Time32(TimeUnit::Millisecond)) => {
            let tz = tz.as_ref().map(|tz| tz.parse()).transpose()?;
            Ok(Arc::new(
                array.as_primitive::<TimestampSecondType>()
                    .try_unary::<_, Time32MillisecondType, ArrowError>(|x| {
                        Ok(time_to_time32ms(as_time_res_with_timezone::<
                            TimestampSecondType,
                        >(x, tz)?))
                    })?,
            ))
        }
        (Timestamp(TimeUnit::Millisecond, tz), Time32(TimeUnit::Second)) => {
            let tz = tz.as_ref().map(|tz| tz.parse()).transpose()?;
            Ok(Arc::new(
                array.as_primitive::<TimestampMillisecondType>()
                    .try_unary::<_, Time32SecondType, ArrowError>(|x| {
                        Ok(time_to_time32s(as_time_res_with_timezone::<
                            TimestampMillisecondType,
                        >(x, tz)?))
                    })?,
            ))
        }
        (Timestamp(TimeUnit::Millisecond, tz), Time32(TimeUnit::Millisecond)) => {
            let tz = tz.as_ref().map(|tz| tz.parse()).transpose()?;
            Ok(Arc::new(
                array.as_primitive::<TimestampMillisecondType>()
                    .try_unary::<_, Time32MillisecondType, ArrowError>(|x| {
                        Ok(time_to_time32ms(as_time_res_with_timezone::<
                            TimestampMillisecondType,
                        >(x, tz)?))
                    })?,
            ))
        }
        (Timestamp(TimeUnit::Microsecond, tz), Time32(TimeUnit::Second)) => {
            let tz = tz.as_ref().map(|tz| tz.parse()).transpose()?;
            Ok(Arc::new(
                array.as_primitive::<TimestampMicrosecondType>()
                    .try_unary::<_, Time32SecondType, ArrowError>(|x| {
                        Ok(time_to_time32s(as_time_res_with_timezone::<
                            TimestampMicrosecondType,
                        >(x, tz)?))
                    })?,
            ))
        }
        (Timestamp(TimeUnit::Microsecond, tz), Time32(TimeUnit::Millisecond)) => {
            let tz = tz.as_ref().map(|tz| tz.parse()).transpose()?;
            Ok(Arc::new(
                array.as_primitive::<TimestampMicrosecondType>()
                    .try_unary::<_, Time32MillisecondType, ArrowError>(|x| {
                        Ok(time_to_time32ms(as_time_res_with_timezone::<
                            TimestampMicrosecondType,
                        >(x, tz)?))
                    })?,
            ))
        }
        (Timestamp(TimeUnit::Nanosecond, tz), Time32(TimeUnit::Second)) => {
            let tz = tz.as_ref().map(|tz| tz.parse()).transpose()?;
            Ok(Arc::new(
                array.as_primitive::<TimestampNanosecondType>()
                    .try_unary::<_, Time32SecondType, ArrowError>(|x| {
                        Ok(time_to_time32s(as_time_res_with_timezone::<
                            TimestampNanosecondType,
                        >(x, tz)?))
                    })?,
            ))
        }
        (Timestamp(TimeUnit::Nanosecond, tz), Time32(TimeUnit::Millisecond)) => {
            let tz = tz.as_ref().map(|tz| tz.parse()).transpose()?;
            Ok(Arc::new(
                array.as_primitive::<TimestampNanosecondType>()
                    .try_unary::<_, Time32MillisecondType, ArrowError>(|x| {
                        Ok(time_to_time32ms(as_time_res_with_timezone::<
                            TimestampNanosecondType,
                        >(x, tz)?))
                    })?,
            ))
        }

        (Date64, Timestamp(TimeUnit::Second, None)) => Ok(Arc::new(
            array.as_primitive::<Date64Type>()
                .unary::<_, TimestampSecondType>(|x| x / MILLISECONDS),
        )),
        (Date64, Timestamp(TimeUnit::Millisecond, None)) => {
            cast_reinterpret_arrays::<Date64Type, TimestampMillisecondType>(array)
        }
        (Date64, Timestamp(TimeUnit::Microsecond, None)) => Ok(Arc::new(
            array.as_primitive::<Date64Type>().unary::<_, TimestampMicrosecondType>(
                |x| x * (MICROSECONDS / MILLISECONDS),
            ),
        )),
        (Date64, Timestamp(TimeUnit::Nanosecond, None)) => Ok(Arc::new(
            array.as_primitive::<Date64Type>().unary::<_, TimestampNanosecondType>(
                |x| x * (NANOSECONDS / MILLISECONDS),
            ),
        )),
        (Date32, Timestamp(TimeUnit::Second, None)) => Ok(Arc::new(
            array.as_primitive::<Date32Type>()
                .unary::<_, TimestampSecondType>(|x| (x as i64) * SECONDS_IN_DAY),
        )),
        (Date32, Timestamp(TimeUnit::Millisecond, None)) => Ok(Arc::new(
            array.as_primitive::<Date32Type>().unary::<_, TimestampMillisecondType>(
                |x| (x as i64) * MILLISECONDS_IN_DAY,
            ),
        )),
        (Date32, Timestamp(TimeUnit::Microsecond, None)) => Ok(Arc::new(
            array.as_primitive::<Date32Type>().unary::<_, TimestampMicrosecondType>(
                |x| (x as i64) * MICROSECONDS_IN_DAY,
            ),
        )),
        (Date32, Timestamp(TimeUnit::Nanosecond, None)) => Ok(Arc::new(
            array.as_primitive::<Date32Type>()
                .unary::<_, TimestampNanosecondType>(|x| (x as i64) * NANOSECONDS_IN_DAY),
        )),
        (Int64, Duration(TimeUnit::Second)) => {
            cast_reinterpret_arrays::<Int64Type, DurationSecondType>(array)
        }
        (Int64, Duration(TimeUnit::Millisecond)) => {
            cast_reinterpret_arrays::<Int64Type, DurationMillisecondType>(array)
        }
        (Int64, Duration(TimeUnit::Microsecond)) => {
            cast_reinterpret_arrays::<Int64Type, DurationMicrosecondType>(array)
        }
        (Int64, Duration(TimeUnit::Nanosecond)) => {
            cast_reinterpret_arrays::<Int64Type, DurationNanosecondType>(array)
        }

        (Duration(TimeUnit::Second), Int64) => {
            cast_reinterpret_arrays::<DurationSecondType, Int64Type>(array)
        }
        (Duration(TimeUnit::Millisecond), Int64) => {
            cast_reinterpret_arrays::<DurationMillisecondType, Int64Type>(array)
        }
        (Duration(TimeUnit::Microsecond), Int64) => {
            cast_reinterpret_arrays::<DurationMicrosecondType, Int64Type>(array)
        }
        (Duration(TimeUnit::Nanosecond), Int64) => {
            cast_reinterpret_arrays::<DurationNanosecondType, Int64Type>(array)
        }
        (Duration(TimeUnit::Second), Interval(IntervalUnit::MonthDayNano)) => {
            cast_duration_to_interval::<DurationSecondType>(array, cast_options)
        }
        (Duration(TimeUnit::Millisecond), Interval(IntervalUnit::MonthDayNano)) => {
            cast_duration_to_interval::<DurationMillisecondType>(array, cast_options)
        }
        (Duration(TimeUnit::Microsecond), Interval(IntervalUnit::MonthDayNano)) => {
            cast_duration_to_interval::<DurationMicrosecondType>(array, cast_options)
        }
        (Duration(TimeUnit::Nanosecond), Interval(IntervalUnit::MonthDayNano)) => {
            cast_duration_to_interval::<DurationNanosecondType>(array, cast_options)
        }
        (DataType::Interval(IntervalUnit::MonthDayNano), DataType::Duration(TimeUnit::Second)) => {
            cast_interval_to_duration::<DurationSecondType>(array, cast_options)
        }
        (DataType::Interval(IntervalUnit::MonthDayNano), DataType::Duration(TimeUnit::Millisecond)) => {
            cast_interval_to_duration::<DurationMillisecondType>(array, cast_options)
        }
        (DataType::Interval(IntervalUnit::MonthDayNano), DataType::Duration(TimeUnit::Microsecond)) => {
            cast_interval_to_duration::<DurationMicrosecondType>(array, cast_options)
        }
        (DataType::Interval(IntervalUnit::MonthDayNano), DataType::Duration(TimeUnit::Nanosecond)) => {
            cast_interval_to_duration::<DurationNanosecondType>(array, cast_options)
        }
        (Interval(IntervalUnit::YearMonth), Int64) => {
            cast_numeric_arrays::<IntervalYearMonthType, Int64Type>(array, cast_options)
        }
        (Interval(IntervalUnit::DayTime), Int64) => {
            cast_reinterpret_arrays::<IntervalDayTimeType, Int64Type>(array)
        }
        (Int32, Interval(IntervalUnit::YearMonth)) => {
            cast_reinterpret_arrays::<Int32Type, IntervalYearMonthType>(array)
        }
        (Int64, Interval(IntervalUnit::DayTime)) => {
            cast_reinterpret_arrays::<Int64Type, IntervalDayTimeType>(array)
        }
        (_, _) => Err(ArrowError::CastError(format!(
            "Casting from {from_type:?} to {to_type:?} not supported",
        ))),
    }
}

/// Get the time unit as a multiple of a second
const fn time_unit_multiple(unit: &TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => MILLISECONDS,
        TimeUnit::Microsecond => MICROSECONDS,
        TimeUnit::Nanosecond => NANOSECONDS,
    }
}

/// A utility trait that provides checked conversions between
/// decimal types inspired by [`NumCast`]
trait DecimalCast: Sized {
    fn to_i128(self) -> Option<i128>;

    fn to_i256(self) -> Option<i256>;

    fn from_decimal<T: DecimalCast>(n: T) -> Option<Self>;
}

impl DecimalCast for i128 {
    fn to_i128(self) -> Option<i128> {
        Some(self)
    }

    fn to_i256(self) -> Option<i256> {
        Some(i256::from_i128(self))
    }

    fn from_decimal<T: DecimalCast>(n: T) -> Option<Self> {
        n.to_i128()
    }
}

impl DecimalCast for i256 {
    fn to_i128(self) -> Option<i128> {
        self.to_i128()
    }

    fn to_i256(self) -> Option<i256> {
        Some(self)
    }

    fn from_decimal<T: DecimalCast>(n: T) -> Option<Self> {
        n.to_i256()
    }
}

fn cast_decimal_to_decimal_error<I, O>(
    output_precision: u8,
    output_scale: i8,
) -> impl Fn(<I as ArrowPrimitiveType>::Native) -> ArrowError
where
    I: DecimalType,
    O: DecimalType,
    I::Native: DecimalCast + ArrowNativeTypeOp,
    O::Native: DecimalCast + ArrowNativeTypeOp,
{
    move |x: I::Native| {
        ArrowError::CastError(format!(
            "Cannot cast to {}({}, {}). Overflowing on {:?}",
            O::PREFIX,
            output_precision,
            output_scale,
            x
        ))
    }
}

fn convert_to_smaller_scale_decimal<I, O>(
    array: &PrimitiveArray<I>,
    input_scale: i8,
    output_precision: u8,
    output_scale: i8,
    cast_options: &CastOptions,
) -> Result<PrimitiveArray<O>, ArrowError>
where
    I: DecimalType,
    O: DecimalType,
    I::Native: DecimalCast + ArrowNativeTypeOp,
    O::Native: DecimalCast + ArrowNativeTypeOp,
{
    let error = cast_decimal_to_decimal_error::<I, O>(output_precision, output_scale);
    let div = I::Native::from_decimal(10_i128)
        .unwrap()
        .pow_checked((input_scale - output_scale) as u32)?;

    let half = div.div_wrapping(I::Native::from_usize(2).unwrap());
    let half_neg = half.neg_wrapping();

    let f = |x: I::Native| {
        // div is >= 10 and so this cannot overflow
        let d = x.div_wrapping(div);
        let r = x.mod_wrapping(div);

        // Round result
        let adjusted = match x >= I::Native::ZERO {
            true if r >= half => d.add_wrapping(I::Native::ONE),
            false if r <= half_neg => d.sub_wrapping(I::Native::ONE),
            _ => d,
        };
        O::Native::from_decimal(adjusted)
    };

    Ok(match cast_options.safe {
        true => array.unary_opt(f),
        false => array.try_unary(|x| f(x).ok_or_else(|| error(x)))?,
    })
}

fn convert_to_bigger_or_equal_scale_decimal<I, O>(
    array: &PrimitiveArray<I>,
    input_scale: i8,
    output_precision: u8,
    output_scale: i8,
    cast_options: &CastOptions,
) -> Result<PrimitiveArray<O>, ArrowError>
where
    I: DecimalType,
    O: DecimalType,
    I::Native: DecimalCast + ArrowNativeTypeOp,
    O::Native: DecimalCast + ArrowNativeTypeOp,
{
    let error = cast_decimal_to_decimal_error::<I, O>(output_precision, output_scale);
    let mul = O::Native::from_decimal(10_i128)
        .unwrap()
        .pow_checked((output_scale - input_scale) as u32)?;

    let f = |x| O::Native::from_decimal(x).and_then(|x| x.mul_checked(mul).ok());

    Ok(match cast_options.safe {
        true => array.unary_opt(f),
        false => array.try_unary(|x| f(x).ok_or_else(|| error(x)))?,
    })
}

// Only support one type of decimal cast operations
fn cast_decimal_to_decimal_same_type<T>(
    array: &PrimitiveArray<T>,
    input_scale: i8,
    output_precision: u8,
    output_scale: i8,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    T: DecimalType,
    T::Native: DecimalCast + ArrowNativeTypeOp,
{
    let array: PrimitiveArray<T> = match input_scale.cmp(&output_scale) {
        Ordering::Equal => {
            // the scale doesn't change, the native value don't need to be changed
            array.clone()
        }
        Ordering::Greater => convert_to_smaller_scale_decimal::<T, T>(
            array,
            input_scale,
            output_precision,
            output_scale,
            cast_options,
        )?,
        Ordering::Less => {
            // input_scale < output_scale
            convert_to_bigger_or_equal_scale_decimal::<T, T>(
                array,
                input_scale,
                output_precision,
                output_scale,
                cast_options,
            )?
        }
    };

    Ok(Arc::new(array.with_precision_and_scale(
        output_precision,
        output_scale,
    )?))
}

// Support two different types of decimal cast operations
fn cast_decimal_to_decimal<I, O>(
    array: &PrimitiveArray<I>,
    input_scale: i8,
    output_precision: u8,
    output_scale: i8,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    I: DecimalType,
    O: DecimalType,
    I::Native: DecimalCast + ArrowNativeTypeOp,
    O::Native: DecimalCast + ArrowNativeTypeOp,
{
    let array: PrimitiveArray<O> = if input_scale > output_scale {
        convert_to_smaller_scale_decimal::<I, O>(
            array,
            input_scale,
            output_precision,
            output_scale,
            cast_options,
        )?
    } else {
        convert_to_bigger_or_equal_scale_decimal::<I, O>(
            array,
            input_scale,
            output_precision,
            output_scale,
            cast_options,
        )?
    };

    Ok(Arc::new(array.with_precision_and_scale(
        output_precision,
        output_scale,
    )?))
}

/// Convert Array into a PrimitiveArray of type, and apply numeric cast
fn cast_numeric_arrays<FROM, TO>(
    from: &dyn Array,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    FROM: ArrowPrimitiveType,
    TO: ArrowPrimitiveType,
    FROM::Native: NumCast,
    TO::Native: NumCast,
{
    if cast_options.safe {
        // If the value can't be casted to the `TO::Native`, return null
        Ok(Arc::new(numeric_cast::<FROM, TO>(
            from.as_primitive::<FROM>(),
        )))
    } else {
        // If the value can't be casted to the `TO::Native`, return error
        Ok(Arc::new(try_numeric_cast::<FROM, TO>(
            from.as_primitive::<FROM>(),
        )?))
    }
}

// Natural cast between numeric types
// If the value of T can't be casted to R, will throw error
fn try_numeric_cast<T, R>(
    from: &PrimitiveArray<T>,
) -> Result<PrimitiveArray<R>, ArrowError>
where
    T: ArrowPrimitiveType,
    R: ArrowPrimitiveType,
    T::Native: NumCast,
    R::Native: NumCast,
{
    from.try_unary(|value| {
        num::cast::cast::<T::Native, R::Native>(value).ok_or_else(|| {
            ArrowError::CastError(format!(
                "Can't cast value {:?} to type {}",
                value,
                R::DATA_TYPE
            ))
        })
    })
}

// Natural cast between numeric types
// If the value of T can't be casted to R, it will be converted to null
fn numeric_cast<T, R>(from: &PrimitiveArray<T>) -> PrimitiveArray<R>
where
    T: ArrowPrimitiveType,
    R: ArrowPrimitiveType,
    T::Native: NumCast,
    R::Native: NumCast,
{
    from.unary_opt::<_, R>(num::cast::cast::<T::Native, R::Native>)
}

fn value_to_string<O: OffsetSizeTrait>(
    array: &dyn Array,
) -> Result<ArrayRef, ArrowError> {
    let mut builder = GenericStringBuilder::<O>::new();
    let options = FormatOptions::default();
    let formatter = ArrayFormatter::try_new(array, &options)?;
    let nulls = array.nulls();
    for i in 0..array.len() {
        match nulls.map(|x| x.is_null(i)).unwrap_or_default() {
            true => builder.append_null(),
            false => {
                formatter.value(i).write(&mut builder)?;
                // tell the builder the row is finished
                builder.append_value("");
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Cast numeric types to Utf8
fn cast_string_to_numeric<T, Offset: OffsetSizeTrait>(
    from: &dyn Array,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    T: ArrowPrimitiveType,
    <T as ArrowPrimitiveType>::Native: lexical_core::FromLexical,
{
    Ok(Arc::new(string_to_numeric_cast::<T, Offset>(
        from.as_any()
            .downcast_ref::<GenericStringArray<Offset>>()
            .unwrap(),
        cast_options,
    )?))
}

fn string_to_numeric_cast<T, Offset: OffsetSizeTrait>(
    from: &GenericStringArray<Offset>,
    cast_options: &CastOptions,
) -> Result<PrimitiveArray<T>, ArrowError>
where
    T: ArrowPrimitiveType,
    <T as ArrowPrimitiveType>::Native: lexical_core::FromLexical,
{
    if cast_options.safe {
        let iter = from
            .iter()
            .map(|v| v.and_then(|v| lexical_core::parse(v.as_bytes()).ok()));
        // Benefit:
        //     20% performance improvement
        // Soundness:
        //     The iterator is trustedLen because it comes from an `StringArray`.
        Ok(unsafe { PrimitiveArray::<T>::from_trusted_len_iter(iter) })
    } else {
        let vec = from
            .iter()
            .map(|v| {
                v.map(|v| {
                    lexical_core::parse(v.as_bytes()).map_err(|_| {
                        ArrowError::CastError(format!(
                            "Cannot cast string '{}' to value of {:?} type",
                            v,
                            T::DATA_TYPE,
                        ))
                    })
                })
                .transpose()
            })
            .collect::<Result<Vec<_>, _>>()?;
        // Benefit:
        //     20% performance improvement
        // Soundness:
        //     The iterator is trustedLen because it comes from an `StringArray`.
        Ok(unsafe { PrimitiveArray::<T>::from_trusted_len_iter(vec.iter()) })
    }
}

/// Casts generic string arrays to Date32Array
fn cast_string_to_date32<Offset: OffsetSizeTrait>(
    array: &dyn Array,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    use chrono::Datelike;
    let string_array = array
        .as_any()
        .downcast_ref::<GenericStringArray<Offset>>()
        .unwrap();

    let array = if cast_options.safe {
        let iter = string_array.iter().map(|v| {
            v.and_then(|v| {
                v.parse::<chrono::NaiveDate>()
                    .map(|date| date.num_days_from_ce() - EPOCH_DAYS_FROM_CE)
                    .ok()
            })
        });

        // Benefit:
        //     20% performance improvement
        // Soundness:
        //     The iterator is trustedLen because it comes from an `StringArray`.
        unsafe { Date32Array::from_trusted_len_iter(iter) }
    } else {
        let vec = string_array
            .iter()
            .map(|v| {
                v.map(|v| {
                    v.parse::<chrono::NaiveDate>()
                        .map(|date| date.num_days_from_ce() - EPOCH_DAYS_FROM_CE)
                        .map_err(|_| {
                            ArrowError::CastError(format!(
                                "Cannot cast string '{}' to value of {:?} type",
                                v,
                                DataType::Date32
                            ))
                        })
                })
                .transpose()
            })
            .collect::<Result<Vec<Option<i32>>, _>>()?;

        // Benefit:
        //     20% performance improvement
        // Soundness:
        //     The iterator is trustedLen because it comes from an `StringArray`.
        unsafe { Date32Array::from_trusted_len_iter(vec.iter()) }
    };

    Ok(Arc::new(array) as ArrayRef)
}

/// Casts generic string arrays to Date64Array
fn cast_string_to_date64<Offset: OffsetSizeTrait>(
    array: &dyn Array,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let string_array = array
        .as_any()
        .downcast_ref::<GenericStringArray<Offset>>()
        .unwrap();

    let array = if cast_options.safe {
        let iter = string_array.iter().map(|v| {
            v.and_then(|v| {
                v.parse::<chrono::NaiveDateTime>()
                    .map(|datetime| datetime.timestamp_millis())
                    .ok()
            })
        });

        // Benefit:
        //     20% performance improvement
        // Soundness:
        //     The iterator is trustedLen because it comes from an `StringArray`.
        unsafe { Date64Array::from_trusted_len_iter(iter) }
    } else {
        let vec = string_array
            .iter()
            .map(|v| {
                v.map(|v| {
                    v.parse::<chrono::NaiveDateTime>()
                        .map(|datetime| datetime.timestamp_millis())
                        .map_err(|_| {
                            ArrowError::CastError(format!(
                                "Cannot cast string '{}' to value of {:?} type",
                                v,
                                DataType::Date64
                            ))
                        })
                })
                .transpose()
            })
            .collect::<Result<Vec<Option<i64>>, _>>()?;

        // Benefit:
        //     20% performance improvement
        // Soundness:
        //     The iterator is trustedLen because it comes from an `StringArray`.
        unsafe { Date64Array::from_trusted_len_iter(vec.iter()) }
    };

    Ok(Arc::new(array) as ArrayRef)
}

/// Casts generic string arrays to `Time32SecondArray`
fn cast_string_to_time32second<Offset: OffsetSizeTrait>(
    array: &dyn Array,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    /// The number of nanoseconds per millisecond.
    const NANOS_PER_SEC: u32 = 1_000_000_000;

    let string_array = array
        .as_any()
        .downcast_ref::<GenericStringArray<Offset>>()
        .unwrap();

    let array = if cast_options.safe {
        let iter = string_array.iter().map(|v| {
            v.and_then(|v| {
                v.parse::<chrono::NaiveTime>()
                    .map(|time| {
                        (time.num_seconds_from_midnight()
                            + time.nanosecond() / NANOS_PER_SEC)
                            as i32
                    })
                    .ok()
            })
        });

        // Benefit:
        //     20% performance improvement
        // Soundness:
        //     The iterator is trustedLen because it comes from an `StringArray`.
        unsafe { Time32SecondArray::from_trusted_len_iter(iter) }
    } else {
        let vec = string_array
            .iter()
            .map(|v| {
                v.map(|v| {
                    v.parse::<chrono::NaiveTime>()
                        .map(|time| {
                            (time.num_seconds_from_midnight()
                                + time.nanosecond() / NANOS_PER_SEC)
                                as i32
                        })
                        .map_err(|_| {
                            ArrowError::CastError(format!(
                                "Cannot cast string '{}' to value of {:?} type",
                                v,
                                DataType::Time32(TimeUnit::Second)
                            ))
                        })
                })
                .transpose()
            })
            .collect::<Result<Vec<Option<i32>>, _>>()?;

        // Benefit:
        //     20% performance improvement
        // Soundness:
        //     The iterator is trustedLen because it comes from an `StringArray`.
        unsafe { Time32SecondArray::from_trusted_len_iter(vec.iter()) }
    };

    Ok(Arc::new(array) as ArrayRef)
}

/// Casts generic string arrays to `Time32MillisecondArray`
fn cast_string_to_time32millisecond<Offset: OffsetSizeTrait>(
    array: &dyn Array,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    /// The number of nanoseconds per millisecond.
    const NANOS_PER_MILLI: u32 = 1_000_000;
    /// The number of milliseconds per second.
    const MILLIS_PER_SEC: u32 = 1_000;

    let string_array = array
        .as_any()
        .downcast_ref::<GenericStringArray<Offset>>()
        .unwrap();

    let array = if cast_options.safe {
        let iter = string_array.iter().map(|v| {
            v.and_then(|v| {
                v.parse::<chrono::NaiveTime>()
                    .map(|time| {
                        (time.num_seconds_from_midnight() * MILLIS_PER_SEC
                            + time.nanosecond() / NANOS_PER_MILLI)
                            as i32
                    })
                    .ok()
            })
        });

        // Benefit:
        //     20% performance improvement
        // Soundness:
        //     The iterator is trustedLen because it comes from an `StringArray`.
        unsafe { Time32MillisecondArray::from_trusted_len_iter(iter) }
    } else {
        let vec = string_array
            .iter()
            .map(|v| {
                v.map(|v| {
                    v.parse::<chrono::NaiveTime>()
                        .map(|time| {
                            (time.num_seconds_from_midnight() * MILLIS_PER_SEC
                                + time.nanosecond() / NANOS_PER_MILLI)
                                as i32
                        })
                        .map_err(|_| {
                            ArrowError::CastError(format!(
                                "Cannot cast string '{}' to value of {:?} type",
                                v,
                                DataType::Time32(TimeUnit::Millisecond)
                            ))
                        })
                })
                .transpose()
            })
            .collect::<Result<Vec<Option<i32>>, _>>()?;

        // Benefit:
        //     20% performance improvement
        // Soundness:
        //     The iterator is trustedLen because it comes from an `StringArray`.
        unsafe { Time32MillisecondArray::from_trusted_len_iter(vec.iter()) }
    };

    Ok(Arc::new(array) as ArrayRef)
}

/// Casts generic string arrays to `Time64MicrosecondArray`
fn cast_string_to_time64microsecond<Offset: OffsetSizeTrait>(
    array: &dyn Array,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    /// The number of nanoseconds per microsecond.
    const NANOS_PER_MICRO: i64 = 1_000;
    /// The number of microseconds per second.
    const MICROS_PER_SEC: i64 = 1_000_000;

    let string_array = array
        .as_any()
        .downcast_ref::<GenericStringArray<Offset>>()
        .unwrap();

    let array = if cast_options.safe {
        let iter = string_array.iter().map(|v| {
            v.and_then(|v| {
                v.parse::<chrono::NaiveTime>()
                    .map(|time| {
                        time.num_seconds_from_midnight() as i64 * MICROS_PER_SEC
                            + time.nanosecond() as i64 / NANOS_PER_MICRO
                    })
                    .ok()
            })
        });

        // Benefit:
        //     20% performance improvement
        // Soundness:
        //     The iterator is trustedLen because it comes from an `StringArray`.
        unsafe { Time64MicrosecondArray::from_trusted_len_iter(iter) }
    } else {
        let vec = string_array
            .iter()
            .map(|v| {
                v.map(|v| {
                    v.parse::<chrono::NaiveTime>()
                        .map(|time| {
                            time.num_seconds_from_midnight() as i64 * MICROS_PER_SEC
                                + time.nanosecond() as i64 / NANOS_PER_MICRO
                        })
                        .map_err(|_| {
                            ArrowError::CastError(format!(
                                "Cannot cast string '{}' to value of {:?} type",
                                v,
                                DataType::Time64(TimeUnit::Microsecond)
                            ))
                        })
                })
                .transpose()
            })
            .collect::<Result<Vec<Option<i64>>, _>>()?;

        // Benefit:
        //     20% performance improvement
        // Soundness:
        //     The iterator is trustedLen because it comes from an `StringArray`.
        unsafe { Time64MicrosecondArray::from_trusted_len_iter(vec.iter()) }
    };

    Ok(Arc::new(array) as ArrayRef)
}

/// Casts generic string arrays to `Time64NanosecondArray`
fn cast_string_to_time64nanosecond<Offset: OffsetSizeTrait>(
    array: &dyn Array,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    /// The number of nanoseconds per second.
    const NANOS_PER_SEC: i64 = 1_000_000_000;

    let string_array = array
        .as_any()
        .downcast_ref::<GenericStringArray<Offset>>()
        .unwrap();

    let array = if cast_options.safe {
        let iter = string_array.iter().map(|v| {
            v.and_then(|v| {
                v.parse::<chrono::NaiveTime>()
                    .map(|time| {
                        time.num_seconds_from_midnight() as i64 * NANOS_PER_SEC
                            + time.nanosecond() as i64
                    })
                    .ok()
            })
        });

        // Benefit:
        //     20% performance improvement
        // Soundness:
        //     The iterator is trustedLen because it comes from an `StringArray`.
        unsafe { Time64NanosecondArray::from_trusted_len_iter(iter) }
    } else {
        let vec = string_array
            .iter()
            .map(|v| {
                v.map(|v| {
                    v.parse::<chrono::NaiveTime>()
                        .map(|time| {
                            time.num_seconds_from_midnight() as i64 * NANOS_PER_SEC
                                + time.nanosecond() as i64
                        })
                        .map_err(|_| {
                            ArrowError::CastError(format!(
                                "Cannot cast string '{}' to value of {:?} type",
                                v,
                                DataType::Time64(TimeUnit::Nanosecond)
                            ))
                        })
                })
                .transpose()
            })
            .collect::<Result<Vec<Option<i64>>, _>>()?;

        // Benefit:
        //     20% performance improvement
        // Soundness:
        //     The iterator is trustedLen because it comes from an `StringArray`.
        unsafe { Time64NanosecondArray::from_trusted_len_iter(vec.iter()) }
    };

    Ok(Arc::new(array) as ArrayRef)
}

/// Casts generic string arrays to an ArrowTimestampType (TimeStampNanosecondArray, etc.)
fn cast_string_to_timestamp<O: OffsetSizeTrait, T: ArrowTimestampType>(
    array: &dyn Array,
    to_tz: &Option<Arc<str>>,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let array = array.as_string::<O>();
    let out: PrimitiveArray<T> = match to_tz {
        Some(tz) => {
            let tz: Tz = tz.as_ref().parse()?;
            cast_string_to_timestamp_impl(array, &tz, cast_options)?
        }
        None => cast_string_to_timestamp_impl(array, &Utc, cast_options)?,
    };
    Ok(Arc::new(out.with_timezone_opt(to_tz.clone())))
}

fn cast_string_to_timestamp_impl<
    O: OffsetSizeTrait,
    T: ArrowTimestampType,
    Tz: TimeZone,
>(
    array: &GenericStringArray<O>,
    tz: &Tz,
    cast_options: &CastOptions,
) -> Result<PrimitiveArray<T>, ArrowError> {
    if cast_options.safe {
        let iter = array.iter().map(|v| {
            v.and_then(|v| {
                let naive = string_to_datetime(tz, v).ok()?.naive_utc();
                T::make_value(naive)
            })
        });
        // Benefit:
        //     20% performance improvement
        // Soundness:
        //     The iterator is trustedLen because it comes from an `StringArray`.

        Ok(unsafe { PrimitiveArray::from_trusted_len_iter(iter) })
    } else {
        let vec = array
            .iter()
            .map(|v| {
                v.map(|v| {
                    let naive = string_to_datetime(tz, v)?.naive_utc();
                    T::make_value(naive).ok_or_else(|| {
                        ArrowError::CastError(format!(
                            "Overflow converting {naive} to {:?}",
                            T::UNIT
                        ))
                    })
                })
                .transpose()
            })
            .collect::<Result<Vec<Option<i64>>, _>>()?;

        // Benefit:
        //     20% performance improvement
        // Soundness:
        //     The iterator is trustedLen because it comes from an `StringArray`.
        Ok(unsafe { PrimitiveArray::from_trusted_len_iter(vec.iter()) })
    }
}

fn cast_string_to_year_month_interval<Offset: OffsetSizeTrait>(
    array: &dyn Array,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let string_array = array
        .as_any()
        .downcast_ref::<GenericStringArray<Offset>>()
        .unwrap();
    let interval_array = if cast_options.safe {
        let iter = string_array
            .iter()
            .map(|v| v.and_then(|v| parse_interval_year_month(v).ok()));

        // Benefit:
        //     20% performance improvement
        // Soundness:
        //     The iterator is trustedLen because it comes from an `StringArray`.
        unsafe { IntervalYearMonthArray::from_trusted_len_iter(iter) }
    } else {
        let vec = string_array
            .iter()
            .map(|v| v.map(parse_interval_year_month).transpose())
            .collect::<Result<Vec<_>, ArrowError>>()?;

        // Benefit:
        //     20% performance improvement
        // Soundness:
        //     The iterator is trustedLen because it comes from an `StringArray`.
        unsafe { IntervalYearMonthArray::from_trusted_len_iter(vec) }
    };
    Ok(Arc::new(interval_array) as ArrayRef)
}

fn cast_string_to_day_time_interval<Offset: OffsetSizeTrait>(
    array: &dyn Array,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let string_array = array
        .as_any()
        .downcast_ref::<GenericStringArray<Offset>>()
        .unwrap();
    let interval_array = if cast_options.safe {
        let iter = string_array
            .iter()
            .map(|v| v.and_then(|v| parse_interval_day_time(v).ok()));

        // Benefit:
        //     20% performance improvement
        // Soundness:
        //     The iterator is trustedLen because it comes from an `StringArray`.
        unsafe { IntervalDayTimeArray::from_trusted_len_iter(iter) }
    } else {
        let vec = string_array
            .iter()
            .map(|v| v.map(parse_interval_day_time).transpose())
            .collect::<Result<Vec<_>, ArrowError>>()?;

        // Benefit:
        //     20% performance improvement
        // Soundness:
        //     The iterator is trustedLen because it comes from an `StringArray`.
        unsafe { IntervalDayTimeArray::from_trusted_len_iter(vec) }
    };
    Ok(Arc::new(interval_array) as ArrayRef)
}

fn cast_string_to_month_day_nano_interval<Offset: OffsetSizeTrait>(
    array: &dyn Array,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let string_array = array
        .as_any()
        .downcast_ref::<GenericStringArray<Offset>>()
        .unwrap();
    let interval_array = if cast_options.safe {
        let iter = string_array
            .iter()
            .map(|v| v.and_then(|v| parse_interval_month_day_nano(v).ok()));

        // Benefit:
        //     20% performance improvement
        // Soundness:
        //     The iterator is trustedLen because it comes from an `StringArray`.
        unsafe { IntervalMonthDayNanoArray::from_trusted_len_iter(iter) }
    } else {
        let vec = string_array
            .iter()
            .map(|v| v.map(parse_interval_month_day_nano).transpose())
            .collect::<Result<Vec<_>, ArrowError>>()?;

        // Benefit:
        //     20% performance improvement
        // Soundness:
        //     The iterator is trustedLen because it comes from an `StringArray`.
        unsafe { IntervalMonthDayNanoArray::from_trusted_len_iter(vec) }
    };
    Ok(Arc::new(interval_array) as ArrayRef)
}

/// Casts Utf8 to Boolean
fn cast_utf8_to_boolean<OffsetSize>(
    from: &dyn Array,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    OffsetSize: OffsetSizeTrait,
{
    let array = from
        .as_any()
        .downcast_ref::<GenericStringArray<OffsetSize>>()
        .unwrap();

    let output_array = array
        .iter()
        .map(|value| match value {
            Some(value) => match value.to_ascii_lowercase().trim() {
                "t" | "tr" | "tru" | "true" | "y" | "ye" | "yes" | "on" | "1" => {
                    Ok(Some(true))
                }
                "f" | "fa" | "fal" | "fals" | "false" | "n" | "no" | "of" | "off"
                | "0" => Ok(Some(false)),
                invalid_value => match cast_options.safe {
                    true => Ok(None),
                    false => Err(ArrowError::CastError(format!(
                        "Cannot cast value '{invalid_value}' to value of Boolean type",
                    ))),
                },
            },
            None => Ok(None),
        })
        .collect::<Result<BooleanArray, _>>()?;

    Ok(Arc::new(output_array))
}

/// Parses given string to specified decimal native (i128/i256) based on given
/// scale. Returns an `Err` if it cannot parse given string.
fn parse_string_to_decimal_native<T: DecimalType>(
    value_str: &str,
    scale: usize,
) -> Result<T::Native, ArrowError>
where
    T::Native: DecimalCast + ArrowNativeTypeOp,
{
    let value_str = value_str.trim();
    let parts: Vec<&str> = value_str.split('.').collect();
    if parts.len() > 2 {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Invalid decimal format: {value_str:?}"
        )));
    }

    let integers = parts[0].trim_start_matches('0');
    let decimals = if parts.len() == 2 { parts[1] } else { "" };

    // Adjust decimal based on scale
    let number_decimals = if decimals.len() > scale {
        let decimal_number = i256::from_string(decimals).ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "Cannot parse decimal format: {value_str}"
            ))
        })?;

        let div =
            i256::from_i128(10_i128).pow_checked((decimals.len() - scale) as u32)?;

        let half = div.div_wrapping(i256::from_i128(2));
        let half_neg = half.neg_wrapping();

        let d = decimal_number.div_wrapping(div);
        let r = decimal_number.mod_wrapping(div);

        // Round result
        let adjusted = match decimal_number >= i256::ZERO {
            true if r >= half => d.add_wrapping(i256::ONE),
            false if r <= half_neg => d.sub_wrapping(i256::ONE),
            _ => d,
        };

        let integers = if !integers.is_empty() {
            i256::from_string(integers)
                .ok_or_else(|| {
                    ArrowError::InvalidArgumentError(format!(
                        "Cannot parse decimal format: {value_str}"
                    ))
                })
                .map(|v| {
                    v.mul_wrapping(i256::from_i128(10_i128).pow_wrapping(scale as u32))
                })?
        } else {
            i256::ZERO
        };

        format!("{}", integers.add_wrapping(adjusted))
    } else {
        let padding = if scale > decimals.len() { scale } else { 0 };

        let decimals = format!("{decimals:0<padding$}");
        format!("{integers}{decimals}")
    };

    let value = i256::from_string(number_decimals.as_str()).ok_or_else(|| {
        ArrowError::InvalidArgumentError(format!(
            "Cannot convert {} to {}: Overflow",
            value_str,
            T::PREFIX
        ))
    })?;

    T::Native::from_decimal(value).ok_or_else(|| {
        ArrowError::InvalidArgumentError(format!(
            "Cannot convert {} to {}",
            value_str,
            T::PREFIX
        ))
    })
}

fn string_to_decimal_cast<T, Offset: OffsetSizeTrait>(
    from: &GenericStringArray<Offset>,
    precision: u8,
    scale: i8,
    cast_options: &CastOptions,
) -> Result<PrimitiveArray<T>, ArrowError>
where
    T: DecimalType,
    T::Native: DecimalCast + ArrowNativeTypeOp,
{
    if cast_options.safe {
        let iter = from.iter().map(|v| {
            v.and_then(|v| parse_string_to_decimal_native::<T>(v, scale as usize).ok())
        });
        // Benefit:
        //     20% performance improvement
        // Soundness:
        //     The iterator is trustedLen because it comes from an `StringArray`.
        Ok(unsafe {
            PrimitiveArray::<T>::from_trusted_len_iter(iter)
                .with_precision_and_scale(precision, scale)?
        })
    } else {
        let vec = from
            .iter()
            .map(|v| {
                v.map(|v| {
                    parse_string_to_decimal_native::<T>(v, scale as usize).map_err(|_| {
                        ArrowError::CastError(format!(
                            "Cannot cast string '{}' to value of {:?} type",
                            v,
                            T::DATA_TYPE,
                        ))
                    })
                })
                .transpose()
            })
            .collect::<Result<Vec<_>, _>>()?;
        // Benefit:
        //     20% performance improvement
        // Soundness:
        //     The iterator is trustedLen because it comes from an `StringArray`.
        Ok(unsafe {
            PrimitiveArray::<T>::from_trusted_len_iter(vec.iter())
                .with_precision_and_scale(precision, scale)?
        })
    }
}

/// Cast Utf8 to decimal
fn cast_string_to_decimal<T, Offset: OffsetSizeTrait>(
    from: &dyn Array,
    precision: u8,
    scale: i8,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    T: DecimalType,
    T::Native: DecimalCast + ArrowNativeTypeOp,
{
    if scale < 0 {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Cannot cast string to decimal with negative scale {scale}"
        )));
    }

    if scale > T::MAX_SCALE {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Cannot cast string to decimal greater than maximum scale {}",
            T::MAX_SCALE
        )));
    }

    Ok(Arc::new(string_to_decimal_cast::<T, Offset>(
        from.as_any()
            .downcast_ref::<GenericStringArray<Offset>>()
            .unwrap(),
        precision,
        scale,
        cast_options,
    )?))
}

/// Cast numeric types to Boolean
///
/// Any zero value returns `false` while non-zero returns `true`
fn cast_numeric_to_bool<FROM>(from: &dyn Array) -> Result<ArrayRef, ArrowError>
where
    FROM: ArrowPrimitiveType,
{
    numeric_to_bool_cast::<FROM>(from.as_primitive::<FROM>())
        .map(|to| Arc::new(to) as ArrayRef)
}

fn numeric_to_bool_cast<T>(from: &PrimitiveArray<T>) -> Result<BooleanArray, ArrowError>
where
    T: ArrowPrimitiveType + ArrowPrimitiveType,
{
    let mut b = BooleanBuilder::with_capacity(from.len());

    for i in 0..from.len() {
        if from.is_null(i) {
            b.append_null();
        } else if from.value(i) != T::default_value() {
            b.append_value(true);
        } else {
            b.append_value(false);
        }
    }

    Ok(b.finish())
}

/// Cast Boolean types to numeric
///
/// `false` returns 0 while `true` returns 1
fn cast_bool_to_numeric<TO>(
    from: &dyn Array,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    TO: ArrowPrimitiveType,
    TO::Native: num::cast::NumCast,
{
    Ok(Arc::new(bool_to_numeric_cast::<TO>(
        from.as_any().downcast_ref::<BooleanArray>().unwrap(),
        cast_options,
    )))
}

fn bool_to_numeric_cast<T>(
    from: &BooleanArray,
    _cast_options: &CastOptions,
) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    T::Native: num::NumCast,
{
    let iter = (0..from.len()).map(|i| {
        if from.is_null(i) {
            None
        } else if from.value(i) {
            // a workaround to cast a primitive to T::Native, infallible
            num::cast::cast(1)
        } else {
            Some(T::default_value())
        }
    });
    // Benefit:
    //     20% performance improvement
    // Soundness:
    //     The iterator is trustedLen because it comes from a Range
    unsafe { PrimitiveArray::<T>::from_trusted_len_iter(iter) }
}

/// Attempts to cast an `ArrayDictionary` with index type K into
/// `to_type` for supported types.
///
/// K is the key type
fn dictionary_cast<K: ArrowDictionaryKeyType>(
    array: &dyn Array,
    to_type: &DataType,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    use DataType::*;

    match to_type {
        Dictionary(to_index_type, to_value_type) => {
            let dict_array = array
                .as_any()
                .downcast_ref::<DictionaryArray<K>>()
                .ok_or_else(|| {
                    ArrowError::ComputeError(
                        "Internal Error: Cannot cast dictionary to DictionaryArray of expected type".to_string(),
                    )
                })?;

            let keys_array: ArrayRef =
                Arc::new(PrimitiveArray::<K>::from(dict_array.keys().to_data()));
            let values_array = dict_array.values();
            let cast_keys = cast_with_options(&keys_array, to_index_type, cast_options)?;
            let cast_values =
                cast_with_options(values_array, to_value_type, cast_options)?;

            // Failure to cast keys (because they don't fit in the
            // target type) results in NULL values;
            if cast_keys.null_count() > keys_array.null_count() {
                return Err(ArrowError::ComputeError(format!(
                    "Could not convert {} dictionary indexes from {:?} to {:?}",
                    cast_keys.null_count() - keys_array.null_count(),
                    keys_array.data_type(),
                    to_index_type
                )));
            }

            let data = cast_keys.into_data();
            let builder = data
                .into_builder()
                .data_type(to_type.clone())
                .child_data(vec![cast_values.into_data()]);

            // Safety
            // Cast keys are still valid
            let data = unsafe { builder.build_unchecked() };

            // create the appropriate array type
            let new_array: ArrayRef = match **to_index_type {
                Int8 => Arc::new(DictionaryArray::<Int8Type>::from(data)),
                Int16 => Arc::new(DictionaryArray::<Int16Type>::from(data)),
                Int32 => Arc::new(DictionaryArray::<Int32Type>::from(data)),
                Int64 => Arc::new(DictionaryArray::<Int64Type>::from(data)),
                UInt8 => Arc::new(DictionaryArray::<UInt8Type>::from(data)),
                UInt16 => Arc::new(DictionaryArray::<UInt16Type>::from(data)),
                UInt32 => Arc::new(DictionaryArray::<UInt32Type>::from(data)),
                UInt64 => Arc::new(DictionaryArray::<UInt64Type>::from(data)),
                _ => {
                    return Err(ArrowError::CastError(format!(
                        "Unsupported type {to_index_type:?} for dictionary index"
                    )));
                }
            };

            Ok(new_array)
        }
        _ => unpack_dictionary::<K>(array, to_type, cast_options),
    }
}

// Unpack a dictionary where the keys are of type <K> into a flattened array of type to_type
fn unpack_dictionary<K>(
    array: &dyn Array,
    to_type: &DataType,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    K: ArrowDictionaryKeyType,
{
    let dict_array = array
        .as_any()
        .downcast_ref::<DictionaryArray<K>>()
        .ok_or_else(|| {
            ArrowError::ComputeError(
                "Internal Error: Cannot cast dictionary to DictionaryArray of expected type".to_string(),
            )
        })?;

    // attempt to cast the dict values to the target type
    // use the take kernel to expand out the dictionary
    let cast_dict_values = cast_with_options(dict_array.values(), to_type, cast_options)?;

    // Note take requires first casting the indices to u32
    let keys_array: ArrayRef =
        Arc::new(PrimitiveArray::<K>::from(dict_array.keys().to_data()));
    let indices = cast_with_options(&keys_array, &DataType::UInt32, cast_options)?;
    let u32_indices =
        indices
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| {
                ArrowError::ComputeError(
                    "Internal Error: Cannot cast dict indices to UInt32".to_string(),
                )
            })?;

    take(cast_dict_values.as_ref(), u32_indices, None)
}

/// Attempts to encode an array into an `ArrayDictionary` with index
/// type K and value (dictionary) type value_type
///
/// K is the key type
fn cast_to_dictionary<K: ArrowDictionaryKeyType>(
    array: &dyn Array,
    dict_value_type: &DataType,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    use DataType::*;

    match *dict_value_type {
        Int8 => pack_numeric_to_dictionary::<K, Int8Type>(
            array,
            dict_value_type,
            cast_options,
        ),
        Int16 => pack_numeric_to_dictionary::<K, Int16Type>(
            array,
            dict_value_type,
            cast_options,
        ),
        Int32 => pack_numeric_to_dictionary::<K, Int32Type>(
            array,
            dict_value_type,
            cast_options,
        ),
        Int64 => pack_numeric_to_dictionary::<K, Int64Type>(
            array,
            dict_value_type,
            cast_options,
        ),
        UInt8 => pack_numeric_to_dictionary::<K, UInt8Type>(
            array,
            dict_value_type,
            cast_options,
        ),
        UInt16 => pack_numeric_to_dictionary::<K, UInt16Type>(
            array,
            dict_value_type,
            cast_options,
        ),
        UInt32 => pack_numeric_to_dictionary::<K, UInt32Type>(
            array,
            dict_value_type,
            cast_options,
        ),
        UInt64 => pack_numeric_to_dictionary::<K, UInt64Type>(
            array,
            dict_value_type,
            cast_options,
        ),
        Decimal128(_, _) => pack_numeric_to_dictionary::<K, Decimal128Type>(
            array,
            dict_value_type,
            cast_options,
        ),
        Decimal256(_, _) => pack_numeric_to_dictionary::<K, Decimal256Type>(
            array,
            dict_value_type,
            cast_options,
        ),
        Utf8 => pack_byte_to_dictionary::<K, GenericStringType<i32>>(array, cast_options),
        LargeUtf8 => {
            pack_byte_to_dictionary::<K, GenericStringType<i64>>(array, cast_options)
        }
        Binary => {
            pack_byte_to_dictionary::<K, GenericBinaryType<i32>>(array, cast_options)
        }
        LargeBinary => {
            pack_byte_to_dictionary::<K, GenericBinaryType<i64>>(array, cast_options)
        }
        _ => Err(ArrowError::CastError(format!(
            "Unsupported output type for dictionary packing: {dict_value_type:?}"
        ))),
    }
}

// Packs the data from the primitive array of type <V> to a
// DictionaryArray with keys of type K and values of value_type V
fn pack_numeric_to_dictionary<K, V>(
    array: &dyn Array,
    dict_value_type: &DataType,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    K: ArrowDictionaryKeyType,
    V: ArrowPrimitiveType,
{
    // attempt to cast the source array values to the target value type (the dictionary values type)
    let cast_values = cast_with_options(array, dict_value_type, cast_options)?;
    let values = cast_values.as_primitive::<V>();

    let mut b =
        PrimitiveDictionaryBuilder::<K, V>::with_capacity(values.len(), values.len());

    // copy each element one at a time
    for i in 0..values.len() {
        if values.is_null(i) {
            b.append_null();
        } else {
            b.append(values.value(i))?;
        }
    }
    Ok(Arc::new(b.finish()))
}

// Packs the data as a GenericByteDictionaryBuilder, if possible, with the
// key types of K
fn pack_byte_to_dictionary<K, T>(
    array: &dyn Array,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    K: ArrowDictionaryKeyType,
    T: ByteArrayType,
{
    let cast_values = cast_with_options(array, &T::DATA_TYPE, cast_options)?;
    let values = cast_values
        .as_any()
        .downcast_ref::<GenericByteArray<T>>()
        .unwrap();
    let mut b =
        GenericByteDictionaryBuilder::<K, T>::with_capacity(values.len(), 1024, 1024);

    // copy each element one at a time
    for i in 0..values.len() {
        if values.is_null(i) {
            b.append_null();
        } else {
            b.append(values.value(i))?;
        }
    }
    Ok(Arc::new(b.finish()))
}

/// Helper function that takes a primitive array and casts to a (generic) list array.
fn cast_primitive_to_list<OffsetSize: OffsetSizeTrait + NumCast>(
    array: &dyn Array,
    to: &Field,
    to_type: &DataType,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    // cast primitive to list's primitive
    let cast_array = cast_with_options(array, to.data_type(), cast_options)?;
    // create offsets, where if array.len() = 2, we have [0,1,2]
    // Safety:
    // Length of range can be trusted.
    // Note: could not yet create a generic range in stable Rust.
    let offsets = unsafe {
        MutableBuffer::from_trusted_len_iter(
            (0..=array.len()).map(|i| OffsetSize::from(i).expect("integer")),
        )
    };

    let list_data = unsafe {
        ArrayData::new_unchecked(
            to_type.clone(),
            array.len(),
            Some(cast_array.null_count()),
            cast_array.nulls().map(|b| b.inner().sliced()),
            0,
            vec![offsets.into()],
            vec![cast_array.into_data()],
        )
    };
    let list_array =
        Arc::new(GenericListArray::<OffsetSize>::from(list_data)) as ArrayRef;

    Ok(list_array)
}

/// Helper function that takes an Generic list container and casts the inner datatype.
fn cast_list_inner<OffsetSize: OffsetSizeTrait>(
    array: &dyn Array,
    to: &Field,
    to_type: &DataType,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let data = array.to_data();
    let underlying_array = make_array(data.child_data()[0].clone());
    let cast_array =
        cast_with_options(underlying_array.as_ref(), to.data_type(), cast_options)?;
    let builder = data
        .into_builder()
        .data_type(to_type.clone())
        .child_data(vec![cast_array.into_data()]);

    // Safety
    // Data was valid before
    let array_data = unsafe { builder.build_unchecked() };
    let list = GenericListArray::<OffsetSize>::from(array_data);
    Ok(Arc::new(list) as ArrayRef)
}

/// A specified helper to cast from `GenericBinaryArray` to `GenericStringArray` when they have same
/// offset size so re-encoding offset is unnecessary.
fn cast_binary_to_string<O: OffsetSizeTrait>(
    array: &dyn Array,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let array = array
        .as_any()
        .downcast_ref::<GenericByteArray<GenericBinaryType<O>>>()
        .unwrap();

    match GenericStringArray::<O>::try_from_binary(array.clone()) {
        Ok(a) => Ok(Arc::new(a)),
        Err(e) => match cast_options.safe {
            true => {
                // Fallback to slow method to convert invalid sequences to nulls
                let mut builder = GenericStringBuilder::<O>::with_capacity(
                    array.len(),
                    array.value_data().len(),
                );

                let iter = array
                    .iter()
                    .map(|v| v.and_then(|v| std::str::from_utf8(v).ok()));

                builder.extend(iter);
                Ok(Arc::new(builder.finish()))
            }
            false => Err(e),
        },
    }
}

/// Helper function to cast from one `BinaryArray` or 'LargeBinaryArray' to 'FixedSizeBinaryArray'.
fn cast_binary_to_fixed_size_binary<O: OffsetSizeTrait>(
    array: &dyn Array,
    byte_width: i32,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let array = array.as_binary::<O>();
    let mut builder = FixedSizeBinaryBuilder::with_capacity(array.len(), byte_width);

    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_null();
        } else {
            match builder.append_value(array.value(i)) {
                Ok(_) => {}
                Err(e) => match cast_options.safe {
                    true => builder.append_null(),
                    false => return Err(e),
                },
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Helper function to cast from 'FixedSizeBinaryArray' to one `BinaryArray` or 'LargeBinaryArray'.
/// If the target one is too large for the source array it will return an Error.
fn cast_fixed_size_binary_to_binary<O: OffsetSizeTrait>(
    array: &dyn Array,
    byte_width: i32,
) -> Result<ArrayRef, ArrowError> {
    let array = array
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();

    let offsets: i128 = byte_width as i128 * array.len() as i128;

    let is_binary = matches!(GenericBinaryType::<O>::DATA_TYPE, DataType::Binary);
    if is_binary && offsets > i32::MAX as i128 {
        return Err(ArrowError::ComputeError(
            "FixedSizeBinary array too large to cast to Binary array".to_string(),
        ));
    } else if !is_binary && offsets > i64::MAX as i128 {
        return Err(ArrowError::ComputeError(
            "FixedSizeBinary array too large to cast to LargeBinary array".to_string(),
        ));
    }

    let mut builder = GenericBinaryBuilder::<O>::with_capacity(array.len(), array.len());

    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_null();
        } else {
            builder.append_value(array.value(i));
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Helper function to cast from one `ByteArrayType` to another and vice versa.
/// If the target one (e.g., `LargeUtf8`) is too large for the source array it will return an Error.
fn cast_byte_container<FROM, TO>(array: &dyn Array) -> Result<ArrayRef, ArrowError>
where
    FROM: ByteArrayType,
    TO: ByteArrayType<Native = FROM::Native>,
    FROM::Offset: OffsetSizeTrait + ToPrimitive,
    TO::Offset: OffsetSizeTrait + NumCast,
{
    let data = array.to_data();
    assert_eq!(data.data_type(), &FROM::DATA_TYPE);
    let str_values_buf = data.buffers()[1].clone();
    let offsets = data.buffers()[0].typed_data::<FROM::Offset>();

    let mut offset_builder = BufferBuilder::<TO::Offset>::new(offsets.len());
    offsets
        .iter()
        .try_for_each::<_, Result<_, ArrowError>>(|offset| {
            let offset = <<TO as ByteArrayType>::Offset as NumCast>::from(*offset)
                .ok_or_else(|| {
                    ArrowError::ComputeError(format!(
                        "{}{} array too large to cast to {}{} array",
                        FROM::Offset::PREFIX,
                        FROM::PREFIX,
                        TO::Offset::PREFIX,
                        TO::PREFIX
                    ))
                })?;
            offset_builder.append(offset);
            Ok(())
        })?;

    let offset_buffer = offset_builder.finish();

    let dtype = TO::DATA_TYPE;

    let builder = ArrayData::builder(dtype)
        .offset(array.offset())
        .len(array.len())
        .add_buffer(offset_buffer)
        .add_buffer(str_values_buf)
        .nulls(data.nulls().cloned());

    let array_data = unsafe { builder.build_unchecked() };

    Ok(Arc::new(GenericByteArray::<TO>::from(array_data)))
}

/// Cast the container type of List/Largelist array but not the inner types.
/// This function can leave the value data intact and only has to cast the offset dtypes.
fn cast_list_container<OffsetSizeFrom, OffsetSizeTo>(
    array: &dyn Array,
    _cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    OffsetSizeFrom: OffsetSizeTrait + ToPrimitive,
    OffsetSizeTo: OffsetSizeTrait + NumCast,
{
    let list = array.as_list::<OffsetSizeFrom>();
    // the value data stored by the list
    let values = list.values();

    let out_dtype = match array.data_type() {
        DataType::List(value_type) => {
            assert_eq!(
                std::mem::size_of::<OffsetSizeFrom>(),
                std::mem::size_of::<i32>()
            );
            assert_eq!(
                std::mem::size_of::<OffsetSizeTo>(),
                std::mem::size_of::<i64>()
            );
            DataType::LargeList(value_type.clone())
        }
        DataType::LargeList(value_type) => {
            assert_eq!(
                std::mem::size_of::<OffsetSizeFrom>(),
                std::mem::size_of::<i64>()
            );
            assert_eq!(
                std::mem::size_of::<OffsetSizeTo>(),
                std::mem::size_of::<i32>()
            );
            if values.len() > i32::MAX as usize {
                return Err(ArrowError::ComputeError(
                    "LargeList too large to cast to List".into(),
                ));
            }
            DataType::List(value_type.clone())
        }
        // implementation error
        _ => unreachable!(),
    };

    let iter = list.value_offsets().iter().map(|idx| {
        let idx: OffsetSizeTo = NumCast::from(*idx).unwrap();
        idx
    });

    // SAFETY
    //      A slice produces a trusted length iterator
    let offset_buffer = unsafe { Buffer::from_trusted_len_iter(iter) };

    // wrap up
    let builder = ArrayData::builder(out_dtype)
        .len(list.len())
        .add_buffer(offset_buffer)
        .add_child_data(values.to_data())
        .nulls(list.nulls().cloned());

    let array_data = unsafe { builder.build_unchecked() };
    Ok(Arc::new(GenericListArray::<OffsetSizeTo>::from(array_data)))
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! generate_cast_test_case {
        ($INPUT_ARRAY: expr, $OUTPUT_TYPE_ARRAY: ident, $OUTPUT_TYPE: expr, $OUTPUT_VALUES: expr) => {
            // assert cast type
            let input_array_type = $INPUT_ARRAY.data_type();
            assert!(can_cast_types(input_array_type, $OUTPUT_TYPE));
            let casted_array = cast($INPUT_ARRAY, $OUTPUT_TYPE).unwrap();
            let result_array = casted_array
                .as_any()
                .downcast_ref::<$OUTPUT_TYPE_ARRAY>()
                .unwrap();
            assert_eq!($OUTPUT_TYPE, result_array.data_type());
            assert_eq!(result_array.len(), $OUTPUT_VALUES.len());
            for (i, x) in $OUTPUT_VALUES.iter().enumerate() {
                match x {
                    Some(x) => {
                        assert!(!result_array.is_null(i));
                        assert_eq!(result_array.value(i), *x);
                    }
                    None => {
                        assert!(result_array.is_null(i));
                    }
                }
            }

            let cast_option = CastOptions { safe: false };
            let casted_array_with_option =
                cast_with_options($INPUT_ARRAY, $OUTPUT_TYPE, &cast_option).unwrap();
            let result_array = casted_array_with_option
                .as_any()
                .downcast_ref::<$OUTPUT_TYPE_ARRAY>()
                .unwrap();
            assert_eq!($OUTPUT_TYPE, result_array.data_type());
            assert_eq!(result_array.len(), $OUTPUT_VALUES.len());
            for (i, x) in $OUTPUT_VALUES.iter().enumerate() {
                match x {
                    Some(x) => {
                        assert_eq!(result_array.value(i), *x);
                    }
                    None => {
                        assert!(result_array.is_null(i));
                    }
                }
            }
        };
    }

    fn create_decimal_array(
        array: Vec<Option<i128>>,
        precision: u8,
        scale: i8,
    ) -> Result<Decimal128Array, ArrowError> {
        array
            .into_iter()
            .collect::<Decimal128Array>()
            .with_precision_and_scale(precision, scale)
    }

    fn create_decimal256_array(
        array: Vec<Option<i256>>,
        precision: u8,
        scale: i8,
    ) -> Result<Decimal256Array, ArrowError> {
        array
            .into_iter()
            .collect::<Decimal256Array>()
            .with_precision_and_scale(precision, scale)
    }

    #[test]
    #[cfg(not(feature = "force_validate"))]
    #[should_panic(
        expected = "Cannot cast to Decimal128(20, 3). Overflowing on 57896044618658097711785492504343953926634992332820282019728792003956564819967"
    )]
    fn test_cast_decimal_to_decimal_round_with_error() {
        // decimal256 to decimal128 overflow
        let array = vec![
            Some(i256::from_i128(1123454)),
            Some(i256::from_i128(2123456)),
            Some(i256::from_i128(-3123453)),
            Some(i256::from_i128(-3123456)),
            None,
            Some(i256::MAX),
            Some(i256::MIN),
        ];
        let input_decimal_array = create_decimal256_array(array, 76, 4).unwrap();
        let array = Arc::new(input_decimal_array) as ArrayRef;
        let input_type = DataType::Decimal256(76, 4);
        let output_type = DataType::Decimal128(20, 3);
        assert!(can_cast_types(&input_type, &output_type));
        generate_cast_test_case!(
            &array,
            Decimal128Array,
            &output_type,
            vec![
                Some(112345_i128),
                Some(212346_i128),
                Some(-312345_i128),
                Some(-312346_i128),
                None,
                None,
                None,
            ]
        );
    }

    #[test]
    #[cfg(not(feature = "force_validate"))]
    fn test_cast_decimal_to_decimal_round() {
        let array = vec![
            Some(1123454),
            Some(2123456),
            Some(-3123453),
            Some(-3123456),
            None,
        ];
        let array = create_decimal_array(array, 20, 4).unwrap();
        // decimal128 to decimal128
        let input_type = DataType::Decimal128(20, 4);
        let output_type = DataType::Decimal128(20, 3);
        assert!(can_cast_types(&input_type, &output_type));
        generate_cast_test_case!(
            &array,
            Decimal128Array,
            &output_type,
            vec![
                Some(112345_i128),
                Some(212346_i128),
                Some(-312345_i128),
                Some(-312346_i128),
                None
            ]
        );

        // decimal128 to decimal256
        let input_type = DataType::Decimal128(20, 4);
        let output_type = DataType::Decimal256(20, 3);
        assert!(can_cast_types(&input_type, &output_type));
        generate_cast_test_case!(
            &array,
            Decimal256Array,
            &output_type,
            vec![
                Some(i256::from_i128(112345_i128)),
                Some(i256::from_i128(212346_i128)),
                Some(i256::from_i128(-312345_i128)),
                Some(i256::from_i128(-312346_i128)),
                None
            ]
        );

        // decimal256
        let array = vec![
            Some(i256::from_i128(1123454)),
            Some(i256::from_i128(2123456)),
            Some(i256::from_i128(-3123453)),
            Some(i256::from_i128(-3123456)),
            None,
        ];
        let array = create_decimal256_array(array, 20, 4).unwrap();

        // decimal256 to decimal256
        let input_type = DataType::Decimal256(20, 4);
        let output_type = DataType::Decimal256(20, 3);
        assert!(can_cast_types(&input_type, &output_type));
        generate_cast_test_case!(
            &array,
            Decimal256Array,
            &output_type,
            vec![
                Some(i256::from_i128(112345_i128)),
                Some(i256::from_i128(212346_i128)),
                Some(i256::from_i128(-312345_i128)),
                Some(i256::from_i128(-312346_i128)),
                None
            ]
        );
        // decimal256 to decimal128
        let input_type = DataType::Decimal256(20, 4);
        let output_type = DataType::Decimal128(20, 3);
        assert!(can_cast_types(&input_type, &output_type));
        generate_cast_test_case!(
            &array,
            Decimal128Array,
            &output_type,
            vec![
                Some(112345_i128),
                Some(212346_i128),
                Some(-312345_i128),
                Some(-312346_i128),
                None
            ]
        );
    }

    #[test]
    fn test_cast_decimal128_to_decimal128() {
        let input_type = DataType::Decimal128(20, 3);
        let output_type = DataType::Decimal128(20, 4);
        assert!(can_cast_types(&input_type, &output_type));
        let array = vec![Some(1123456), Some(2123456), Some(3123456), None];
        let array = create_decimal_array(array, 20, 3).unwrap();
        generate_cast_test_case!(
            &array,
            Decimal128Array,
            &output_type,
            vec![
                Some(11234560_i128),
                Some(21234560_i128),
                Some(31234560_i128),
                None
            ]
        );
        // negative test
        let array = vec![Some(123456), None];
        let array = create_decimal_array(array, 10, 0).unwrap();
        let result = cast(&array, &DataType::Decimal128(2, 2));
        assert!(result.is_ok());
        let array = result.unwrap();
        let array: &Decimal128Array = array.as_primitive();
        let err = array.validate_decimal_precision(2);
        assert_eq!("Invalid argument error: 12345600 is too large to store in a Decimal128 of precision 2. Max is 99",
                   err.unwrap_err().to_string());
    }

    #[test]
    fn test_cast_decimal128_to_decimal128_overflow() {
        let input_type = DataType::Decimal128(38, 3);
        let output_type = DataType::Decimal128(38, 38);
        assert!(can_cast_types(&input_type, &output_type));

        let array = vec![Some(i128::MAX)];
        let array = create_decimal_array(array, 38, 3).unwrap();
        let result =
            cast_with_options(&array, &output_type, &CastOptions { safe: false });
        assert_eq!("Cast error: Cannot cast to Decimal128(38, 38). Overflowing on 170141183460469231731687303715884105727",
                   result.unwrap_err().to_string());
    }

    #[test]
    fn test_cast_decimal128_to_decimal256_overflow() {
        let input_type = DataType::Decimal128(38, 3);
        let output_type = DataType::Decimal256(76, 76);
        assert!(can_cast_types(&input_type, &output_type));

        let array = vec![Some(i128::MAX)];
        let array = create_decimal_array(array, 38, 3).unwrap();
        let result =
            cast_with_options(&array, &output_type, &CastOptions { safe: false });
        assert_eq!("Cast error: Cannot cast to Decimal256(76, 76). Overflowing on 170141183460469231731687303715884105727",
                   result.unwrap_err().to_string());
    }

    #[test]
    fn test_cast_decimal128_to_decimal256() {
        let input_type = DataType::Decimal128(20, 3);
        let output_type = DataType::Decimal256(20, 4);
        assert!(can_cast_types(&input_type, &output_type));
        let array = vec![Some(1123456), Some(2123456), Some(3123456), None];
        let array = create_decimal_array(array, 20, 3).unwrap();
        generate_cast_test_case!(
            &array,
            Decimal256Array,
            &output_type,
            vec![
                Some(i256::from_i128(11234560_i128)),
                Some(i256::from_i128(21234560_i128)),
                Some(i256::from_i128(31234560_i128)),
                None
            ]
        );
    }

    #[test]
    fn test_cast_decimal256_to_decimal128_overflow() {
        let input_type = DataType::Decimal256(76, 5);
        let output_type = DataType::Decimal128(38, 7);
        assert!(can_cast_types(&input_type, &output_type));
        let array = vec![Some(i256::from_i128(i128::MAX))];
        let array = create_decimal256_array(array, 76, 5).unwrap();
        let result =
            cast_with_options(&array, &output_type, &CastOptions { safe: false });
        assert_eq!("Cast error: Cannot cast to Decimal128(38, 7). Overflowing on 170141183460469231731687303715884105727",
                   result.unwrap_err().to_string());
    }

    #[test]
    fn test_cast_decimal256_to_decimal256_overflow() {
        let input_type = DataType::Decimal256(76, 5);
        let output_type = DataType::Decimal256(76, 55);
        assert!(can_cast_types(&input_type, &output_type));
        let array = vec![Some(i256::from_i128(i128::MAX))];
        let array = create_decimal256_array(array, 76, 5).unwrap();
        let result =
            cast_with_options(&array, &output_type, &CastOptions { safe: false });
        assert_eq!("Cast error: Cannot cast to Decimal256(76, 55). Overflowing on 170141183460469231731687303715884105727",
                   result.unwrap_err().to_string());
    }

    #[test]
    fn test_cast_decimal256_to_decimal128() {
        let input_type = DataType::Decimal256(20, 3);
        let output_type = DataType::Decimal128(20, 4);
        assert!(can_cast_types(&input_type, &output_type));
        let array = vec![
            Some(i256::from_i128(1123456)),
            Some(i256::from_i128(2123456)),
            Some(i256::from_i128(3123456)),
            None,
        ];
        let array = create_decimal256_array(array, 20, 3).unwrap();
        generate_cast_test_case!(
            &array,
            Decimal128Array,
            &output_type,
            vec![
                Some(11234560_i128),
                Some(21234560_i128),
                Some(31234560_i128),
                None
            ]
        );
    }

    #[test]
    fn test_cast_decimal256_to_decimal256() {
        let input_type = DataType::Decimal256(20, 3);
        let output_type = DataType::Decimal256(20, 4);
        assert!(can_cast_types(&input_type, &output_type));
        let array = vec![
            Some(i256::from_i128(1123456)),
            Some(i256::from_i128(2123456)),
            Some(i256::from_i128(3123456)),
            None,
        ];
        let array = create_decimal256_array(array, 20, 3).unwrap();
        generate_cast_test_case!(
            &array,
            Decimal256Array,
            &output_type,
            vec![
                Some(i256::from_i128(11234560_i128)),
                Some(i256::from_i128(21234560_i128)),
                Some(i256::from_i128(31234560_i128)),
                None
            ]
        );
    }

    #[test]
    fn test_cast_decimal_to_numeric() {
        let value_array: Vec<Option<i128>> =
            vec![Some(125), Some(225), Some(325), None, Some(525)];
        let array = create_decimal_array(value_array, 38, 2).unwrap();
        // u8
        generate_cast_test_case!(
            &array,
            UInt8Array,
            &DataType::UInt8,
            vec![Some(1_u8), Some(2_u8), Some(3_u8), None, Some(5_u8)]
        );
        // u16
        generate_cast_test_case!(
            &array,
            UInt16Array,
            &DataType::UInt16,
            vec![Some(1_u16), Some(2_u16), Some(3_u16), None, Some(5_u16)]
        );
        // u32
        generate_cast_test_case!(
            &array,
            UInt32Array,
            &DataType::UInt32,
            vec![Some(1_u32), Some(2_u32), Some(3_u32), None, Some(5_u32)]
        );
        // u64
        generate_cast_test_case!(
            &array,
            UInt64Array,
            &DataType::UInt64,
            vec![Some(1_u64), Some(2_u64), Some(3_u64), None, Some(5_u64)]
        );
        // i8
        generate_cast_test_case!(
            &array,
            Int8Array,
            &DataType::Int8,
            vec![Some(1_i8), Some(2_i8), Some(3_i8), None, Some(5_i8)]
        );
        // i16
        generate_cast_test_case!(
            &array,
            Int16Array,
            &DataType::Int16,
            vec![Some(1_i16), Some(2_i16), Some(3_i16), None, Some(5_i16)]
        );
        // i32
        generate_cast_test_case!(
            &array,
            Int32Array,
            &DataType::Int32,
            vec![Some(1_i32), Some(2_i32), Some(3_i32), None, Some(5_i32)]
        );
        // i64
        generate_cast_test_case!(
            &array,
            Int64Array,
            &DataType::Int64,
            vec![Some(1_i64), Some(2_i64), Some(3_i64), None, Some(5_i64)]
        );
        // f32
        generate_cast_test_case!(
            &array,
            Float32Array,
            &DataType::Float32,
            vec![
                Some(1.25_f32),
                Some(2.25_f32),
                Some(3.25_f32),
                None,
                Some(5.25_f32)
            ]
        );
        // f64
        generate_cast_test_case!(
            &array,
            Float64Array,
            &DataType::Float64,
            vec![
                Some(1.25_f64),
                Some(2.25_f64),
                Some(3.25_f64),
                None,
                Some(5.25_f64)
            ]
        );

        // overflow test: out of range of max u8
        let value_array: Vec<Option<i128>> = vec![Some(51300)];
        let array = create_decimal_array(value_array, 38, 2).unwrap();
        let casted_array =
            cast_with_options(&array, &DataType::UInt8, &CastOptions { safe: false });
        assert_eq!(
            "Cast error: value of 513 is out of range UInt8".to_string(),
            casted_array.unwrap_err().to_string()
        );

        let casted_array =
            cast_with_options(&array, &DataType::UInt8, &CastOptions { safe: true });
        assert!(casted_array.is_ok());
        assert!(casted_array.unwrap().is_null(0));

        // overflow test: out of range of max i8
        let value_array: Vec<Option<i128>> = vec![Some(24400)];
        let array = create_decimal_array(value_array, 38, 2).unwrap();
        let casted_array =
            cast_with_options(&array, &DataType::Int8, &CastOptions { safe: false });
        assert_eq!(
            "Cast error: value of 244 is out of range Int8".to_string(),
            casted_array.unwrap_err().to_string()
        );

        let casted_array =
            cast_with_options(&array, &DataType::Int8, &CastOptions { safe: true });
        assert!(casted_array.is_ok());
        assert!(casted_array.unwrap().is_null(0));

        // loss the precision: convert decimal to f32、f64
        // f32
        // 112345678_f32 and 112345679_f32 are same, so the 112345679_f32 will lose precision.
        let value_array: Vec<Option<i128>> = vec![
            Some(125),
            Some(225),
            Some(325),
            None,
            Some(525),
            Some(112345678),
            Some(112345679),
        ];
        let array = create_decimal_array(value_array, 38, 2).unwrap();
        generate_cast_test_case!(
            &array,
            Float32Array,
            &DataType::Float32,
            vec![
                Some(1.25_f32),
                Some(2.25_f32),
                Some(3.25_f32),
                None,
                Some(5.25_f32),
                Some(1_123_456.7_f32),
                Some(1_123_456.7_f32)
            ]
        );

        // f64
        // 112345678901234568_f64 and 112345678901234560_f64 are same, so the 112345678901234568_f64 will lose precision.
        let value_array: Vec<Option<i128>> = vec![
            Some(125),
            Some(225),
            Some(325),
            None,
            Some(525),
            Some(112345678901234568),
            Some(112345678901234560),
        ];
        let array = create_decimal_array(value_array, 38, 2).unwrap();
        generate_cast_test_case!(
            &array,
            Float64Array,
            &DataType::Float64,
            vec![
                Some(1.25_f64),
                Some(2.25_f64),
                Some(3.25_f64),
                None,
                Some(5.25_f64),
                Some(1_123_456_789_012_345.6_f64),
                Some(1_123_456_789_012_345.6_f64),
            ]
        );
    }

    #[test]
    fn test_cast_decimal256_to_numeric() {
        let value_array: Vec<Option<i256>> = vec![
            Some(i256::from_i128(125)),
            Some(i256::from_i128(225)),
            Some(i256::from_i128(325)),
            None,
            Some(i256::from_i128(525)),
        ];
        let array = create_decimal256_array(value_array, 38, 2).unwrap();
        // u8
        generate_cast_test_case!(
            &array,
            UInt8Array,
            &DataType::UInt8,
            vec![Some(1_u8), Some(2_u8), Some(3_u8), None, Some(5_u8)]
        );
        // u16
        generate_cast_test_case!(
            &array,
            UInt16Array,
            &DataType::UInt16,
            vec![Some(1_u16), Some(2_u16), Some(3_u16), None, Some(5_u16)]
        );
        // u32
        generate_cast_test_case!(
            &array,
            UInt32Array,
            &DataType::UInt32,
            vec![Some(1_u32), Some(2_u32), Some(3_u32), None, Some(5_u32)]
        );
        // u64
        generate_cast_test_case!(
            &array,
            UInt64Array,
            &DataType::UInt64,
            vec![Some(1_u64), Some(2_u64), Some(3_u64), None, Some(5_u64)]
        );
        // i8
        generate_cast_test_case!(
            &array,
            Int8Array,
            &DataType::Int8,
            vec![Some(1_i8), Some(2_i8), Some(3_i8), None, Some(5_i8)]
        );
        // i16
        generate_cast_test_case!(
            &array,
            Int16Array,
            &DataType::Int16,
            vec![Some(1_i16), Some(2_i16), Some(3_i16), None, Some(5_i16)]
        );
        // i32
        generate_cast_test_case!(
            &array,
            Int32Array,
            &DataType::Int32,
            vec![Some(1_i32), Some(2_i32), Some(3_i32), None, Some(5_i32)]
        );
        // i64
        generate_cast_test_case!(
            &array,
            Int64Array,
            &DataType::Int64,
            vec![Some(1_i64), Some(2_i64), Some(3_i64), None, Some(5_i64)]
        );
        // f32
        generate_cast_test_case!(
            &array,
            Float32Array,
            &DataType::Float32,
            vec![
                Some(1.25_f32),
                Some(2.25_f32),
                Some(3.25_f32),
                None,
                Some(5.25_f32)
            ]
        );
        // f64
        generate_cast_test_case!(
            &array,
            Float64Array,
            &DataType::Float64,
            vec![
                Some(1.25_f64),
                Some(2.25_f64),
                Some(3.25_f64),
                None,
                Some(5.25_f64)
            ]
        );

        // overflow test: out of range of max i8
        let value_array: Vec<Option<i256>> = vec![Some(i256::from_i128(24400))];
        let array = create_decimal256_array(value_array, 38, 2).unwrap();
        let casted_array =
            cast_with_options(&array, &DataType::Int8, &CastOptions { safe: false });
        assert_eq!(
            "Cast error: value of 244 is out of range Int8".to_string(),
            casted_array.unwrap_err().to_string()
        );

        let casted_array =
            cast_with_options(&array, &DataType::Int8, &CastOptions { safe: true });
        assert!(casted_array.is_ok());
        assert!(casted_array.unwrap().is_null(0));

        // loss the precision: convert decimal to f32、f64
        // f32
        // 112345678_f32 and 112345679_f32 are same, so the 112345679_f32 will lose precision.
        let value_array: Vec<Option<i256>> = vec![
            Some(i256::from_i128(125)),
            Some(i256::from_i128(225)),
            Some(i256::from_i128(325)),
            None,
            Some(i256::from_i128(525)),
            Some(i256::from_i128(112345678)),
            Some(i256::from_i128(112345679)),
        ];
        let array = create_decimal256_array(value_array, 76, 2).unwrap();
        generate_cast_test_case!(
            &array,
            Float32Array,
            &DataType::Float32,
            vec![
                Some(1.25_f32),
                Some(2.25_f32),
                Some(3.25_f32),
                None,
                Some(5.25_f32),
                Some(1_123_456.7_f32),
                Some(1_123_456.7_f32)
            ]
        );

        // f64
        // 112345678901234568_f64 and 112345678901234560_f64 are same, so the 112345678901234568_f64 will lose precision.
        let value_array: Vec<Option<i256>> = vec![
            Some(i256::from_i128(125)),
            Some(i256::from_i128(225)),
            Some(i256::from_i128(325)),
            None,
            Some(i256::from_i128(525)),
            Some(i256::from_i128(112345678901234568)),
            Some(i256::from_i128(112345678901234560)),
        ];
        let array = create_decimal256_array(value_array, 76, 2).unwrap();
        generate_cast_test_case!(
            &array,
            Float64Array,
            &DataType::Float64,
            vec![
                Some(1.25_f64),
                Some(2.25_f64),
                Some(3.25_f64),
                None,
                Some(5.25_f64),
                Some(1_123_456_789_012_345.6_f64),
                Some(1_123_456_789_012_345.6_f64),
            ]
        );
    }

    #[test]
    fn test_cast_numeric_to_decimal128() {
        let decimal_type = DataType::Decimal128(38, 6);
        // u8, u16, u32, u64
        let input_datas = vec![
            Arc::new(UInt8Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // u8
            Arc::new(UInt16Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // u16
            Arc::new(UInt32Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // u32
            Arc::new(UInt64Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // u64
        ];

        for array in input_datas {
            generate_cast_test_case!(
                &array,
                Decimal128Array,
                &decimal_type,
                vec![
                    Some(1000000_i128),
                    Some(2000000_i128),
                    Some(3000000_i128),
                    None,
                    Some(5000000_i128)
                ]
            );
        }

        // i8, i16, i32, i64
        let input_datas = vec![
            Arc::new(Int8Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // i8
            Arc::new(Int16Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // i16
            Arc::new(Int32Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // i32
            Arc::new(Int64Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // i64
        ];
        for array in input_datas {
            generate_cast_test_case!(
                &array,
                Decimal128Array,
                &decimal_type,
                vec![
                    Some(1000000_i128),
                    Some(2000000_i128),
                    Some(3000000_i128),
                    None,
                    Some(5000000_i128)
                ]
            );
        }

        // test u8 to decimal type with overflow the result type
        // the 100 will be converted to 1000_i128, but it is out of range for max value in the precision 3.
        let array = UInt8Array::from(vec![1, 2, 3, 4, 100]);
        let casted_array = cast(&array, &DataType::Decimal128(3, 1));
        assert!(casted_array.is_ok());
        let array = casted_array.unwrap();
        let array: &Decimal128Array = array.as_primitive();
        assert!(array.is_null(4));

        // test i8 to decimal type with overflow the result type
        // the 100 will be converted to 1000_i128, but it is out of range for max value in the precision 3.
        let array = Int8Array::from(vec![1, 2, 3, 4, 100]);
        let casted_array = cast(&array, &DataType::Decimal128(3, 1));
        assert!(casted_array.is_ok());
        let array = casted_array.unwrap();
        let array: &Decimal128Array = array.as_primitive();
        assert!(array.is_null(4));

        // test f32 to decimal type
        let array = Float32Array::from(vec![
            Some(1.1),
            Some(2.2),
            Some(4.4),
            None,
            Some(1.123_456_4), // round down
            Some(1.123_456_7), // round up
        ]);
        let array = Arc::new(array) as ArrayRef;
        generate_cast_test_case!(
            &array,
            Decimal128Array,
            &decimal_type,
            vec![
                Some(1100000_i128),
                Some(2200000_i128),
                Some(4400000_i128),
                None,
                Some(1123456_i128), // round down
                Some(1123457_i128), // round up
            ]
        );

        // test f64 to decimal type
        let array = Float64Array::from(vec![
            Some(1.1),
            Some(2.2),
            Some(4.4),
            None,
            Some(1.123_456_489_123_4),     // round up
            Some(1.123_456_789_123_4),     // round up
            Some(1.123_456_489_012_345_6), // round down
            Some(1.123_456_789_012_345_6), // round up
        ]);
        generate_cast_test_case!(
            &array,
            Decimal128Array,
            &decimal_type,
            vec![
                Some(1100000_i128),
                Some(2200000_i128),
                Some(4400000_i128),
                None,
                Some(1123456_i128), // round down
                Some(1123457_i128), // round up
                Some(1123456_i128), // round down
                Some(1123457_i128), // round up
            ]
        );
    }

    #[test]
    fn test_cast_numeric_to_decimal256() {
        let decimal_type = DataType::Decimal256(76, 6);
        // u8, u16, u32, u64
        let input_datas = vec![
            Arc::new(UInt8Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // u8
            Arc::new(UInt16Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // u16
            Arc::new(UInt32Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // u32
            Arc::new(UInt64Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // u64
        ];

        for array in input_datas {
            generate_cast_test_case!(
                &array,
                Decimal256Array,
                &decimal_type,
                vec![
                    Some(i256::from_i128(1000000_i128)),
                    Some(i256::from_i128(2000000_i128)),
                    Some(i256::from_i128(3000000_i128)),
                    None,
                    Some(i256::from_i128(5000000_i128))
                ]
            );
        }

        // i8, i16, i32, i64
        let input_datas = vec![
            Arc::new(Int8Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // i8
            Arc::new(Int16Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // i16
            Arc::new(Int32Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // i32
            Arc::new(Int64Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
                None,
                Some(5),
            ])) as ArrayRef, // i64
        ];
        for array in input_datas {
            generate_cast_test_case!(
                &array,
                Decimal256Array,
                &decimal_type,
                vec![
                    Some(i256::from_i128(1000000_i128)),
                    Some(i256::from_i128(2000000_i128)),
                    Some(i256::from_i128(3000000_i128)),
                    None,
                    Some(i256::from_i128(5000000_i128))
                ]
            );
        }

        // test i8 to decimal type with overflow the result type
        // the 100 will be converted to 1000_i128, but it is out of range for max value in the precision 3.
        let array = Int8Array::from(vec![1, 2, 3, 4, 100]);
        let array = Arc::new(array) as ArrayRef;
        let casted_array = cast(&array, &DataType::Decimal256(3, 1));
        assert!(casted_array.is_ok());
        let array = casted_array.unwrap();
        let array: &Decimal256Array = array.as_primitive();
        assert!(array.is_null(4));

        // test f32 to decimal type
        let array = Float32Array::from(vec![
            Some(1.1),
            Some(2.2),
            Some(4.4),
            None,
            Some(1.123_456_4), // round down
            Some(1.123_456_7), // round up
        ]);
        generate_cast_test_case!(
            &array,
            Decimal256Array,
            &decimal_type,
            vec![
                Some(i256::from_i128(1100000_i128)),
                Some(i256::from_i128(2200000_i128)),
                Some(i256::from_i128(4400000_i128)),
                None,
                Some(i256::from_i128(1123456_i128)), // round down
                Some(i256::from_i128(1123457_i128)), // round up
            ]
        );

        // test f64 to decimal type
        let array = Float64Array::from(vec![
            Some(1.1),
            Some(2.2),
            Some(4.4),
            None,
            Some(1.123_456_489_123_4),     // round down
            Some(1.123_456_789_123_4),     // round up
            Some(1.123_456_489_012_345_6), // round down
            Some(1.123_456_789_012_345_6), // round up
        ]);
        generate_cast_test_case!(
            &array,
            Decimal256Array,
            &decimal_type,
            vec![
                Some(i256::from_i128(1100000_i128)),
                Some(i256::from_i128(2200000_i128)),
                Some(i256::from_i128(4400000_i128)),
                None,
                Some(i256::from_i128(1123456_i128)), // round down
                Some(i256::from_i128(1123457_i128)), // round up
                Some(i256::from_i128(1123456_i128)), // round down
                Some(i256::from_i128(1123457_i128)), // round up
            ]
        );
    }

    #[test]
    fn test_cast_i32_to_f64() {
        let array = Int32Array::from(vec![5, 6, 7, 8, 9]);
        let b = cast(&array, &DataType::Float64).unwrap();
        let c = b.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(5.0, c.value(0));
        assert_eq!(6.0, c.value(1));
        assert_eq!(7.0, c.value(2));
        assert_eq!(8.0, c.value(3));
        assert_eq!(9.0, c.value(4));
    }

    #[test]
    fn test_cast_i32_to_u8() {
        let array = Int32Array::from(vec![-5, 6, -7, 8, 100000000]);
        let b = cast(&array, &DataType::UInt8).unwrap();
        let c = b.as_any().downcast_ref::<UInt8Array>().unwrap();
        assert!(!c.is_valid(0));
        assert_eq!(6, c.value(1));
        assert!(!c.is_valid(2));
        assert_eq!(8, c.value(3));
        // overflows return None
        assert!(!c.is_valid(4));
    }

    #[test]
    #[should_panic(expected = "Can't cast value -5 to type UInt8")]
    fn test_cast_int32_to_u8_with_error() {
        let array = Int32Array::from(vec![-5, 6, -7, 8, 100000000]);
        // overflow with the error
        let cast_option = CastOptions { safe: false };
        let result = cast_with_options(&array, &DataType::UInt8, &cast_option);
        assert!(result.is_err());
        result.unwrap();
    }

    #[test]
    fn test_cast_i32_to_u8_sliced() {
        let array = Int32Array::from(vec![-5, 6, -7, 8, 100000000]);
        assert_eq!(0, array.offset());
        let array = array.slice(2, 3);
        let b = cast(&array, &DataType::UInt8).unwrap();
        assert_eq!(3, b.len());
        let c = b.as_any().downcast_ref::<UInt8Array>().unwrap();
        assert!(!c.is_valid(0));
        assert_eq!(8, c.value(1));
        // overflows return None
        assert!(!c.is_valid(2));
    }

    #[test]
    fn test_cast_i32_to_i32() {
        let array = Int32Array::from(vec![5, 6, 7, 8, 9]);
        let b = cast(&array, &DataType::Int32).unwrap();
        let c = b.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(5, c.value(0));
        assert_eq!(6, c.value(1));
        assert_eq!(7, c.value(2));
        assert_eq!(8, c.value(3));
        assert_eq!(9, c.value(4));
    }

    #[test]
    fn test_cast_i32_to_list_i32() {
        let array = Int32Array::from(vec![5, 6, 7, 8, 9]);
        let b = cast(
            &array,
            &DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        )
        .unwrap();
        assert_eq!(5, b.len());
        let arr = b.as_list::<i32>();
        assert_eq!(&[0, 1, 2, 3, 4, 5], arr.value_offsets());
        assert_eq!(1, arr.value_length(0));
        assert_eq!(1, arr.value_length(1));
        assert_eq!(1, arr.value_length(2));
        assert_eq!(1, arr.value_length(3));
        assert_eq!(1, arr.value_length(4));
        let c = arr.values().as_primitive::<Int32Type>();
        assert_eq!(5, c.value(0));
        assert_eq!(6, c.value(1));
        assert_eq!(7, c.value(2));
        assert_eq!(8, c.value(3));
        assert_eq!(9, c.value(4));
    }

    #[test]
    fn test_cast_i32_to_list_i32_nullable() {
        let array = Int32Array::from(vec![Some(5), None, Some(7), Some(8), Some(9)]);
        let b = cast(
            &array,
            &DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        )
        .unwrap();
        assert_eq!(5, b.len());
        assert_eq!(1, b.null_count());
        let arr = b.as_list::<i32>();
        assert_eq!(&[0, 1, 2, 3, 4, 5], arr.value_offsets());
        assert_eq!(1, arr.value_length(0));
        assert_eq!(1, arr.value_length(1));
        assert_eq!(1, arr.value_length(2));
        assert_eq!(1, arr.value_length(3));
        assert_eq!(1, arr.value_length(4));

        let c = arr.values().as_primitive::<Int32Type>();
        assert_eq!(1, c.null_count());
        assert_eq!(5, c.value(0));
        assert!(!c.is_valid(1));
        assert_eq!(7, c.value(2));
        assert_eq!(8, c.value(3));
        assert_eq!(9, c.value(4));
    }

    #[test]
    fn test_cast_i32_to_list_f64_nullable_sliced() {
        let array =
            Int32Array::from(vec![Some(5), None, Some(7), Some(8), None, Some(10)]);
        let array = array.slice(2, 4);
        let b = cast(
            &array,
            &DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
        )
        .unwrap();
        assert_eq!(4, b.len());
        assert_eq!(1, b.null_count());
        let arr = b.as_list::<i32>();
        assert_eq!(&[0, 1, 2, 3, 4], arr.value_offsets());
        assert_eq!(1, arr.value_length(0));
        assert_eq!(1, arr.value_length(1));
        assert_eq!(1, arr.value_length(2));
        assert_eq!(1, arr.value_length(3));
        let c = arr.values().as_primitive::<Float64Type>();
        assert_eq!(1, c.null_count());
        assert_eq!(7.0, c.value(0));
        assert_eq!(8.0, c.value(1));
        assert!(!c.is_valid(2));
        assert_eq!(10.0, c.value(3));
    }

    #[test]
    fn test_cast_utf8_to_i32() {
        let array = StringArray::from(vec!["5", "6", "seven", "8", "9.1"]);
        let b = cast(&array, &DataType::Int32).unwrap();
        let c = b.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(5, c.value(0));
        assert_eq!(6, c.value(1));
        assert!(!c.is_valid(2));
        assert_eq!(8, c.value(3));
        assert!(!c.is_valid(4));
    }

    #[test]
    fn test_cast_with_options_utf8_to_i32() {
        let array = StringArray::from(vec!["5", "6", "seven", "8", "9.1"]);
        let result =
            cast_with_options(&array, &DataType::Int32, &CastOptions { safe: false });
        match result {
            Ok(_) => panic!("expected error"),
            Err(e) => {
                assert!(
                    e.to_string().contains(
                        "Cast error: Cannot cast string 'seven' to value of Int32 type",
                    ),
                    "Error: {e}"
                )
            }
        }
    }

    #[test]
    fn test_cast_utf8_to_bool() {
        let strings = StringArray::from(vec!["true", "false", "invalid", " Y ", ""]);
        let casted = cast(&strings, &DataType::Boolean).unwrap();
        let expected =
            BooleanArray::from(vec![Some(true), Some(false), None, Some(true), None]);
        assert_eq!(*as_boolean_array(&casted), expected);
    }

    #[test]
    fn test_cast_with_options_utf8_to_bool() {
        let strings = StringArray::from(vec!["true", "false", "invalid", " Y ", ""]);
        let casted =
            cast_with_options(&strings, &DataType::Boolean, &CastOptions { safe: false });
        match casted {
            Ok(_) => panic!("expected error"),
            Err(e) => {
                assert!(e.to_string().contains(
                    "Cast error: Cannot cast value 'invalid' to value of Boolean type"
                ))
            }
        }
    }

    #[test]
    fn test_cast_bool_to_i32() {
        let array = BooleanArray::from(vec![Some(true), Some(false), None]);
        let b = cast(&array, &DataType::Int32).unwrap();
        let c = b.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(1, c.value(0));
        assert_eq!(0, c.value(1));
        assert!(!c.is_valid(2));
    }

    #[test]
    fn test_cast_bool_to_f64() {
        let array = BooleanArray::from(vec![Some(true), Some(false), None]);
        let b = cast(&array, &DataType::Float64).unwrap();
        let c = b.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(1.0, c.value(0));
        assert_eq!(0.0, c.value(1));
        assert!(!c.is_valid(2));
    }

    #[test]
    #[should_panic(
        expected = "Casting from Int32 to Timestamp(Microsecond, None) not supported"
    )]
    fn test_cast_int32_to_timestamp() {
        let array = Int32Array::from(vec![Some(2), Some(10), None]);
        cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();
    }

    #[test]
    fn test_cast_list_i32_to_list_u16() {
        let value_data =
            Int32Array::from(vec![0, 0, 0, -1, -2, -1, 2, 100000000]).into_data();

        let value_offsets = Buffer::from_slice_ref([0, 3, 6, 8]);

        // Construct a list array from the above two
        // [[0,0,0], [-1, -2, -1], [2, 100000000]]
        let list_data_type =
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .build()
            .unwrap();
        let list_array = ListArray::from(list_data);

        let cast_array = cast(
            &list_array,
            &DataType::List(Arc::new(Field::new("item", DataType::UInt16, true))),
        )
        .unwrap();

        // For the ListArray itself, there are no null values (as there were no nulls when they went in)
        //
        // 3 negative values should get lost when casting to unsigned,
        // 1 value should overflow
        assert_eq!(0, cast_array.null_count());

        // offsets should be the same
        let array = cast_array.as_list::<i32>();
        assert_eq!(list_array.value_offsets(), array.value_offsets());

        assert_eq!(DataType::UInt16, array.value_type());
        assert_eq!(3, array.value_length(0));
        assert_eq!(3, array.value_length(1));
        assert_eq!(2, array.value_length(2));

        // expect 4 nulls: negative numbers and overflow
        let u16arr = array.values().as_primitive::<UInt16Type>();
        assert_eq!(4, u16arr.null_count());

        // expect 4 nulls: negative numbers and overflow
        let expected: UInt16Array =
            vec![Some(0), Some(0), Some(0), None, None, None, Some(2), None]
                .into_iter()
                .collect();

        assert_eq!(u16arr, &expected);
    }

    #[test]
    #[should_panic(
        expected = "Casting from Int32 to Timestamp(Microsecond, None) not supported"
    )]
    fn test_cast_list_i32_to_list_timestamp() {
        // Construct a value array
        let value_data =
            Int32Array::from(vec![0, 0, 0, -1, -2, -1, 2, 8, 100000000]).into_data();

        let value_offsets = Buffer::from_slice_ref([0, 3, 6, 9]);

        // Construct a list array from the above two
        let list_data_type =
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .build()
            .unwrap();
        let list_array = Arc::new(ListArray::from(list_data)) as ArrayRef;

        cast(
            &list_array,
            &DataType::List(Arc::new(Field::new(
                "item",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ))),
        )
        .unwrap();
    }

    #[test]
    fn test_cast_date32_to_date64() {
        let a = Date32Array::from(vec![10000, 17890]);
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Date64).unwrap();
        let c = b.as_any().downcast_ref::<Date64Array>().unwrap();
        assert_eq!(864000000000, c.value(0));
        assert_eq!(1545696000000, c.value(1));
    }

    #[test]
    fn test_cast_date64_to_date32() {
        let a = Date64Array::from(vec![Some(864000000005), Some(1545696000001), None]);
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Date32).unwrap();
        let c = b.as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(10000, c.value(0));
        assert_eq!(17890, c.value(1));
        assert!(c.is_null(2));
    }

    #[test]
    fn test_cast_string_to_timestamp() {
        let a1 = Arc::new(StringArray::from(vec![
            Some("2020-09-08T12:00:00.123456789+00:00"),
            Some("Not a valid date"),
            None,
        ])) as ArrayRef;
        let a2 = Arc::new(LargeStringArray::from(vec![
            Some("2020-09-08T12:00:00.123456789+00:00"),
            Some("Not a valid date"),
            None,
        ])) as ArrayRef;
        for array in &[a1, a2] {
            for time_unit in &[
                TimeUnit::Second,
                TimeUnit::Millisecond,
                TimeUnit::Microsecond,
                TimeUnit::Nanosecond,
            ] {
                let to_type = DataType::Timestamp(time_unit.clone(), None);
                let b = cast(array, &to_type).unwrap();

                match time_unit {
                    TimeUnit::Second => {
                        let c =
                            b.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
                        assert_eq!(1599566400, c.value(0));
                        assert!(c.is_null(1));
                        assert!(c.is_null(2));
                    }
                    TimeUnit::Millisecond => {
                        let c = b
                            .as_any()
                            .downcast_ref::<TimestampMillisecondArray>()
                            .unwrap();
                        assert_eq!(1599566400123, c.value(0));
                        assert!(c.is_null(1));
                        assert!(c.is_null(2));
                    }
                    TimeUnit::Microsecond => {
                        let c = b
                            .as_any()
                            .downcast_ref::<TimestampMicrosecondArray>()
                            .unwrap();
                        assert_eq!(1599566400123456, c.value(0));
                        assert!(c.is_null(1));
                        assert!(c.is_null(2));
                    }
                    TimeUnit::Nanosecond => {
                        let c = b
                            .as_any()
                            .downcast_ref::<TimestampNanosecondArray>()
                            .unwrap();
                        assert_eq!(1599566400123456789, c.value(0));
                        assert!(c.is_null(1));
                        assert!(c.is_null(2));
                    }
                }

                let options = CastOptions { safe: false };
                let err = cast_with_options(array, &to_type, &options).unwrap_err();
                assert_eq!(
                    err.to_string(),
                    "Parser error: Error parsing timestamp from 'Not a valid date': error parsing date"
                );
            }
        }
    }

    #[test]
    fn test_cast_string_to_timestamp_overflow() {
        let array = StringArray::from(vec!["9800-09-08T12:00:00.123456789"]);
        let result = cast(&array, &DataType::Timestamp(TimeUnit::Second, None)).unwrap();
        let result = result.as_primitive::<TimestampSecondType>();
        assert_eq!(result.values(), &[247112596800]);
    }

    #[test]
    fn test_cast_string_to_date32() {
        let a1 = Arc::new(StringArray::from(vec![
            Some("2018-12-25"),
            Some("Not a valid date"),
            None,
        ])) as ArrayRef;
        let a2 = Arc::new(LargeStringArray::from(vec![
            Some("2018-12-25"),
            Some("Not a valid date"),
            None,
        ])) as ArrayRef;
        for array in &[a1, a2] {
            let to_type = DataType::Date32;
            let b = cast(array, &to_type).unwrap();
            let c = b.as_any().downcast_ref::<Date32Array>().unwrap();
            assert_eq!(17890, c.value(0));
            assert!(c.is_null(1));
            assert!(c.is_null(2));

            let options = CastOptions { safe: false };
            let err = cast_with_options(array, &to_type, &options).unwrap_err();
            assert_eq!(err.to_string(), "Cast error: Cannot cast string 'Not a valid date' to value of Date32 type");
        }
    }

    #[test]
    fn test_cast_string_to_time32second() {
        let a1 = Arc::new(StringArray::from(vec![
            Some("08:08:35.091323414"),
            Some("08:08:60.091323414"), // leap second
            Some("08:08:61.091323414"), // not valid
            Some("Not a valid time"),
            None,
        ])) as ArrayRef;
        let a2 = Arc::new(LargeStringArray::from(vec![
            Some("08:08:35.091323414"),
            Some("08:08:60.091323414"), // leap second
            Some("08:08:61.091323414"), // not valid
            Some("Not a valid time"),
            None,
        ])) as ArrayRef;
        for array in &[a1, a2] {
            let to_type = DataType::Time32(TimeUnit::Second);
            let b = cast(array, &to_type).unwrap();
            let c = b.as_any().downcast_ref::<Time32SecondArray>().unwrap();
            assert_eq!(29315, c.value(0));
            assert_eq!(29340, c.value(1));
            assert!(c.is_null(2));
            assert!(c.is_null(3));
            assert!(c.is_null(4));

            let options = CastOptions { safe: false };
            let err = cast_with_options(array, &to_type, &options).unwrap_err();
            assert_eq!(err.to_string(), "Cast error: Cannot cast string '08:08:61.091323414' to value of Time32(Second) type");
        }
    }

    #[test]
    fn test_cast_string_to_time32millisecond() {
        let a1 = Arc::new(StringArray::from(vec![
            Some("08:08:35.091323414"),
            Some("08:08:60.091323414"), // leap second
            Some("08:08:61.091323414"), // not valid
            Some("Not a valid time"),
            None,
        ])) as ArrayRef;
        let a2 = Arc::new(LargeStringArray::from(vec![
            Some("08:08:35.091323414"),
            Some("08:08:60.091323414"), // leap second
            Some("08:08:61.091323414"), // not valid
            Some("Not a valid time"),
            None,
        ])) as ArrayRef;
        for array in &[a1, a2] {
            let to_type = DataType::Time32(TimeUnit::Millisecond);
            let b = cast(array, &to_type).unwrap();
            let c = b.as_any().downcast_ref::<Time32MillisecondArray>().unwrap();
            assert_eq!(29315091, c.value(0));
            assert_eq!(29340091, c.value(1));
            assert!(c.is_null(2));
            assert!(c.is_null(3));
            assert!(c.is_null(4));

            let options = CastOptions { safe: false };
            let err = cast_with_options(array, &to_type, &options).unwrap_err();
            assert_eq!(err.to_string(), "Cast error: Cannot cast string '08:08:61.091323414' to value of Time32(Millisecond) type");
        }
    }

    #[test]
    fn test_cast_string_to_time64microsecond() {
        let a1 = Arc::new(StringArray::from(vec![
            Some("08:08:35.091323414"),
            Some("Not a valid time"),
            None,
        ])) as ArrayRef;
        let a2 = Arc::new(LargeStringArray::from(vec![
            Some("08:08:35.091323414"),
            Some("Not a valid time"),
            None,
        ])) as ArrayRef;
        for array in &[a1, a2] {
            let to_type = DataType::Time64(TimeUnit::Microsecond);
            let b = cast(array, &to_type).unwrap();
            let c = b.as_any().downcast_ref::<Time64MicrosecondArray>().unwrap();
            assert_eq!(29315091323, c.value(0));
            assert!(c.is_null(1));
            assert!(c.is_null(2));

            let options = CastOptions { safe: false };
            let err = cast_with_options(array, &to_type, &options).unwrap_err();
            assert_eq!(err.to_string(), "Cast error: Cannot cast string 'Not a valid time' to value of Time64(Microsecond) type");
        }
    }

    #[test]
    fn test_cast_string_to_time64nanosecond() {
        let a1 = Arc::new(StringArray::from(vec![
            Some("08:08:35.091323414"),
            Some("Not a valid time"),
            None,
        ])) as ArrayRef;
        let a2 = Arc::new(LargeStringArray::from(vec![
            Some("08:08:35.091323414"),
            Some("Not a valid time"),
            None,
        ])) as ArrayRef;
        for array in &[a1, a2] {
            let to_type = DataType::Time64(TimeUnit::Nanosecond);
            let b = cast(array, &to_type).unwrap();
            let c = b.as_any().downcast_ref::<Time64NanosecondArray>().unwrap();
            assert_eq!(29315091323414, c.value(0));
            assert!(c.is_null(1));
            assert!(c.is_null(2));

            let options = CastOptions { safe: false };
            let err = cast_with_options(array, &to_type, &options).unwrap_err();
            assert_eq!(err.to_string(), "Cast error: Cannot cast string 'Not a valid time' to value of Time64(Nanosecond) type");
        }
    }

    #[test]
    fn test_cast_string_to_date64() {
        let a1 = Arc::new(StringArray::from(vec![
            Some("2020-09-08T12:00:00"),
            Some("Not a valid date"),
            None,
        ])) as ArrayRef;
        let a2 = Arc::new(LargeStringArray::from(vec![
            Some("2020-09-08T12:00:00"),
            Some("Not a valid date"),
            None,
        ])) as ArrayRef;
        for array in &[a1, a2] {
            let to_type = DataType::Date64;
            let b = cast(array, &to_type).unwrap();
            let c = b.as_any().downcast_ref::<Date64Array>().unwrap();
            assert_eq!(1599566400000, c.value(0));
            assert!(c.is_null(1));
            assert!(c.is_null(2));

            let options = CastOptions { safe: false };
            let err = cast_with_options(array, &to_type, &options).unwrap_err();
            assert_eq!(err.to_string(), "Cast error: Cannot cast string 'Not a valid date' to value of Date64 type");
        }
    }

    macro_rules! test_safe_string_to_interval {
        ($data_vec:expr, $interval_unit:expr, $array_ty:ty, $expect_vec:expr) => {
            let source_string_array =
                Arc::new(StringArray::from($data_vec.clone())) as ArrayRef;

            let options = CastOptions { safe: true };

            let target_interval_array = cast_with_options(
                &source_string_array.clone(),
                &DataType::Interval($interval_unit),
                &options,
            )
            .unwrap()
            .as_any()
            .downcast_ref::<$array_ty>()
            .unwrap()
            .clone() as $array_ty;

            let target_string_array =
                cast_with_options(&target_interval_array, &DataType::Utf8, &options)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .clone();

            let expect_string_array = StringArray::from($expect_vec);

            assert_eq!(target_string_array, expect_string_array);

            let target_large_string_array =
                cast_with_options(&target_interval_array, &DataType::LargeUtf8, &options)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .unwrap()
                    .clone();

            let expect_large_string_array = LargeStringArray::from($expect_vec);

            assert_eq!(target_large_string_array, expect_large_string_array);
        };
    }

    #[test]
    fn test_cast_string_to_interval_year_month() {
        test_safe_string_to_interval!(
            vec![
                Some("1 year 1 month"),
                Some("1.5 years 13 month"),
                Some("30 days"),
                Some("31 days"),
                Some("2 months 31 days"),
                Some("2 months 31 days 1 second"),
                Some("foobar"),
            ],
            IntervalUnit::YearMonth,
            IntervalYearMonthArray,
            vec![
                Some("1 years 1 mons 0 days 0 hours 0 mins 0.00 secs"),
                Some("2 years 7 mons 0 days 0 hours 0 mins 0.00 secs"),
                None,
                None,
                None,
                None,
                None,
            ]
        );
    }

    #[test]
    fn test_cast_string_to_interval_day_time() {
        test_safe_string_to_interval!(
            vec![
                Some("1 year 1 month"),
                Some("1.5 years 13 month"),
                Some("30 days"),
                Some("1 day 2 second 3.5 milliseconds"),
                Some("foobar"),
            ],
            IntervalUnit::DayTime,
            IntervalDayTimeArray,
            vec![
                Some("0 years 0 mons 390 days 0 hours 0 mins 0.000 secs"),
                Some("0 years 0 mons 930 days 0 hours 0 mins 0.000 secs"),
                Some("0 years 0 mons 30 days 0 hours 0 mins 0.000 secs"),
                None,
                None,
            ]
        );
    }

    #[test]
    fn test_cast_string_to_interval_month_day_nano() {
        test_safe_string_to_interval!(
            vec![
                Some("1 year 1 month 1 day"),
                None,
                Some("1.5 years 13 month 35 days 1.4 milliseconds"),
                Some("3 days"),
                Some("8 seconds"),
                None,
                Some("1 day 29800 milliseconds"),
                Some("3 months 1 second"),
                Some("6 minutes 120 second"),
                Some("2 years 39 months 9 days 19 hours 1 minute 83 seconds 399222 milliseconds"),
                Some("foobar"),
            ],
            IntervalUnit::MonthDayNano,
            IntervalMonthDayNanoArray,
            vec![
                Some("0 years 13 mons 1 days 0 hours 0 mins 0.000000000 secs"),
                None,
                Some("0 years 31 mons 35 days 0 hours 0 mins 0.001400000 secs"),
                Some("0 years 0 mons 3 days 0 hours 0 mins 0.000000000 secs"),
                Some("0 years 0 mons 0 days 0 hours 0 mins 8.000000000 secs"),
                None,
                Some("0 years 0 mons 1 days 0 hours 0 mins 29.800000000 secs"),
                Some("0 years 3 mons 0 days 0 hours 0 mins 1.000000000 secs"),
                Some("0 years 0 mons 0 days 0 hours 8 mins 0.000000000 secs"),
                Some("0 years 63 mons 9 days 19 hours 9 mins 2.222000000 secs"),
                None,
            ]
        );
    }

    macro_rules! test_unsafe_string_to_interval_err {
        ($data_vec:expr, $interval_unit:expr, $error_msg:expr) => {
            let string_array = Arc::new(StringArray::from($data_vec.clone())) as ArrayRef;
            let options = CastOptions { safe: false };
            let arrow_err = cast_with_options(
                &string_array.clone(),
                &DataType::Interval($interval_unit),
                &options,
            )
            .unwrap_err();
            assert_eq!($error_msg, arrow_err.to_string());
        };
    }

    #[test]
    fn test_cast_string_to_interval_err() {
        test_unsafe_string_to_interval_err!(
            vec![Some("foobar")],
            IntervalUnit::YearMonth,
            r#"Not yet implemented: Unsupported Interval Expression with value "foobar""#
        );
        test_unsafe_string_to_interval_err!(
            vec![Some("foobar")],
            IntervalUnit::DayTime,
            r#"Not yet implemented: Unsupported Interval Expression with value "foobar""#
        );
        test_unsafe_string_to_interval_err!(
            vec![Some("foobar")],
            IntervalUnit::MonthDayNano,
            r#"Not yet implemented: Unsupported Interval Expression with value "foobar""#
        );
        test_unsafe_string_to_interval_err!(
            vec![Some("2 months 31 days 1 second")],
            IntervalUnit::YearMonth,
            r#"Cast error: Cannot cast 2 months 31 days 1 second to IntervalYearMonth. Only year and month fields are allowed."#
        );
        test_unsafe_string_to_interval_err!(
            vec![Some("1 day 1.5 milliseconds")],
            IntervalUnit::DayTime,
            r#"Cast error: Cannot cast 1 day 1.5 milliseconds to IntervalDayTime because the nanos part isn't multiple of milliseconds"#
        );

        // overflow
        test_unsafe_string_to_interval_err!(
            vec![Some(format!(
                "{} century {} year {} month",
                i64::MAX - 2,
                i64::MAX - 2,
                i64::MAX - 2
            ))],
            IntervalUnit::DayTime,
            r#"Parser error: Parsed interval field value out of range: 11068046444225730000000 months 331764692165666300000000 days 28663672503769583000000000000000000000 nanos"#
        );
        test_unsafe_string_to_interval_err!(
            vec![Some(format!(
                "{} year {} month {} day",
                i64::MAX - 2,
                i64::MAX - 2,
                i64::MAX - 2
            ))],
            IntervalUnit::MonthDayNano,
            r#"Parser error: Parsed interval field value out of range: 110680464442257310000 months 3043712772162076000000 days 262179884170819100000000000000000000 nanos"#
        );
    }

    #[test]
    fn test_cast_binary_to_fixed_size_binary() {
        let bytes_1 = "Hiiii".as_bytes();
        let bytes_2 = "Hello".as_bytes();

        let binary_data = vec![Some(bytes_1), Some(bytes_2), None];
        let a1 = Arc::new(BinaryArray::from(binary_data.clone())) as ArrayRef;
        let a2 = Arc::new(LargeBinaryArray::from(binary_data)) as ArrayRef;

        let array_ref = cast(&a1, &DataType::FixedSizeBinary(5)).unwrap();
        let down_cast = array_ref
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(bytes_1, down_cast.value(0));
        assert_eq!(bytes_2, down_cast.value(1));
        assert!(down_cast.is_null(2));

        let array_ref = cast(&a2, &DataType::FixedSizeBinary(5)).unwrap();
        let down_cast = array_ref
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(bytes_1, down_cast.value(0));
        assert_eq!(bytes_2, down_cast.value(1));
        assert!(down_cast.is_null(2));

        // test error cases when the length of binary are not same
        let bytes_1 = "Hi".as_bytes();
        let bytes_2 = "Hello".as_bytes();

        let binary_data = vec![Some(bytes_1), Some(bytes_2), None];
        let a1 = Arc::new(BinaryArray::from(binary_data.clone())) as ArrayRef;
        let a2 = Arc::new(LargeBinaryArray::from(binary_data)) as ArrayRef;

        let array_ref = cast_with_options(
            &a1,
            &DataType::FixedSizeBinary(5),
            &CastOptions { safe: false },
        );
        assert!(array_ref.is_err());

        let array_ref = cast_with_options(
            &a2,
            &DataType::FixedSizeBinary(5),
            &CastOptions { safe: false },
        );
        assert!(array_ref.is_err());
    }

    #[test]
    fn test_fixed_size_binary_to_binary() {
        let bytes_1 = "Hiiii".as_bytes();
        let bytes_2 = "Hello".as_bytes();

        let binary_data = vec![Some(bytes_1), Some(bytes_2), None];
        let a1 = Arc::new(FixedSizeBinaryArray::from(binary_data.clone())) as ArrayRef;

        let array_ref = cast(&a1, &DataType::Binary).unwrap();
        let down_cast = array_ref.as_binary::<i32>();
        assert_eq!(bytes_1, down_cast.value(0));
        assert_eq!(bytes_2, down_cast.value(1));
        assert!(down_cast.is_null(2));

        let array_ref = cast(&a1, &DataType::LargeBinary).unwrap();
        let down_cast = array_ref.as_binary::<i64>();
        assert_eq!(bytes_1, down_cast.value(0));
        assert_eq!(bytes_2, down_cast.value(1));
        assert!(down_cast.is_null(2));
    }

    #[test]
    fn test_cast_date32_to_int32() {
        let array = Date32Array::from(vec![10000, 17890]);
        let b = cast(&array, &DataType::Int32).unwrap();
        let c = b.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(10000, c.value(0));
        assert_eq!(17890, c.value(1));
    }

    #[test]
    fn test_cast_int32_to_date32() {
        let array = Int32Array::from(vec![10000, 17890]);
        let b = cast(&array, &DataType::Date32).unwrap();
        let c = b.as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(10000, c.value(0));
        assert_eq!(17890, c.value(1));
    }

    #[test]
    fn test_cast_timestamp_to_date32() {
        let array = TimestampMillisecondArray::from(vec![
            Some(864000000005),
            Some(1545696000001),
            None,
        ])
        .with_timezone("UTC".to_string());
        let b = cast(&array, &DataType::Date32).unwrap();
        let c = b.as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(10000, c.value(0));
        assert_eq!(17890, c.value(1));
        assert!(c.is_null(2));
    }

    #[test]
    fn test_cast_timestamp_to_date64() {
        let array = TimestampMillisecondArray::from(vec![
            Some(864000000005),
            Some(1545696000001),
            None,
        ]);
        let b = cast(&array, &DataType::Date64).unwrap();
        let c = b.as_any().downcast_ref::<Date64Array>().unwrap();
        assert_eq!(864000000005, c.value(0));
        assert_eq!(1545696000001, c.value(1));
        assert!(c.is_null(2));

        let array =
            TimestampSecondArray::from(vec![Some(864000000005), Some(1545696000001)]);
        let b = cast(&array, &DataType::Date64).unwrap();
        let c = b.as_any().downcast_ref::<Date64Array>().unwrap();
        assert_eq!(864000000005000, c.value(0));
        assert_eq!(1545696000001000, c.value(1));

        // test overflow, safe cast
        let array = TimestampSecondArray::from(vec![Some(i64::MAX)]);
        let b = cast(&array, &DataType::Date64).unwrap();
        assert!(b.is_null(0));
        // test overflow, unsafe cast
        let array = TimestampSecondArray::from(vec![Some(i64::MAX)]);
        let options = CastOptions { safe: false };
        let b = cast_with_options(&array, &DataType::Date64, &options);
        assert!(b.is_err());
    }

    #[test]
    fn test_cast_timestamp_to_time64() {
        // test timestamp secs
        let array = TimestampSecondArray::from(vec![Some(86405), Some(1), None])
            .with_timezone("+01:00".to_string());
        let b = cast(&array, &DataType::Time64(TimeUnit::Microsecond)).unwrap();
        let c = b.as_any().downcast_ref::<Time64MicrosecondArray>().unwrap();
        assert_eq!(3605000000, c.value(0));
        assert_eq!(3601000000, c.value(1));
        assert!(c.is_null(2));
        let b = cast(&array, &DataType::Time64(TimeUnit::Nanosecond)).unwrap();
        let c = b.as_any().downcast_ref::<Time64NanosecondArray>().unwrap();
        assert_eq!(3605000000000, c.value(0));
        assert_eq!(3601000000000, c.value(1));
        assert!(c.is_null(2));

        // test timestamp milliseconds
        let a = TimestampMillisecondArray::from(vec![Some(86405000), Some(1000), None])
            .with_timezone("+01:00".to_string());
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Time64(TimeUnit::Microsecond)).unwrap();
        let c = b.as_any().downcast_ref::<Time64MicrosecondArray>().unwrap();
        assert_eq!(3605000000, c.value(0));
        assert_eq!(3601000000, c.value(1));
        assert!(c.is_null(2));
        let b = cast(&array, &DataType::Time64(TimeUnit::Nanosecond)).unwrap();
        let c = b.as_any().downcast_ref::<Time64NanosecondArray>().unwrap();
        assert_eq!(3605000000000, c.value(0));
        assert_eq!(3601000000000, c.value(1));
        assert!(c.is_null(2));

        // test timestamp microseconds
        let a =
            TimestampMicrosecondArray::from(vec![Some(86405000000), Some(1000000), None])
                .with_timezone("+01:00".to_string());
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Time64(TimeUnit::Microsecond)).unwrap();
        let c = b.as_any().downcast_ref::<Time64MicrosecondArray>().unwrap();
        assert_eq!(3605000000, c.value(0));
        assert_eq!(3601000000, c.value(1));
        assert!(c.is_null(2));
        let b = cast(&array, &DataType::Time64(TimeUnit::Nanosecond)).unwrap();
        let c = b.as_any().downcast_ref::<Time64NanosecondArray>().unwrap();
        assert_eq!(3605000000000, c.value(0));
        assert_eq!(3601000000000, c.value(1));
        assert!(c.is_null(2));

        // test timestamp nanoseconds
        let a = TimestampNanosecondArray::from(vec![
            Some(86405000000000),
            Some(1000000000),
            None,
        ])
        .with_timezone("+01:00".to_string());
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Time64(TimeUnit::Microsecond)).unwrap();
        let c = b.as_any().downcast_ref::<Time64MicrosecondArray>().unwrap();
        assert_eq!(3605000000, c.value(0));
        assert_eq!(3601000000, c.value(1));
        assert!(c.is_null(2));
        let b = cast(&array, &DataType::Time64(TimeUnit::Nanosecond)).unwrap();
        let c = b.as_any().downcast_ref::<Time64NanosecondArray>().unwrap();
        assert_eq!(3605000000000, c.value(0));
        assert_eq!(3601000000000, c.value(1));
        assert!(c.is_null(2));

        // test overflow
        let a = TimestampSecondArray::from(vec![Some(i64::MAX)])
            .with_timezone("+01:00".to_string());
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Time64(TimeUnit::Microsecond));
        assert!(b.is_err());
        let b = cast(&array, &DataType::Time64(TimeUnit::Nanosecond));
        assert!(b.is_err());
        let b = cast(&array, &DataType::Time64(TimeUnit::Millisecond));
        assert!(b.is_err());
    }

    #[test]
    fn test_cast_timestamp_to_time32() {
        // test timestamp secs
        let a = TimestampSecondArray::from(vec![Some(86405), Some(1), None])
            .with_timezone("+01:00".to_string());
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Time32(TimeUnit::Second)).unwrap();
        let c = b.as_any().downcast_ref::<Time32SecondArray>().unwrap();
        assert_eq!(3605, c.value(0));
        assert_eq!(3601, c.value(1));
        assert!(c.is_null(2));
        let b = cast(&array, &DataType::Time32(TimeUnit::Millisecond)).unwrap();
        let c = b.as_any().downcast_ref::<Time32MillisecondArray>().unwrap();
        assert_eq!(3605000, c.value(0));
        assert_eq!(3601000, c.value(1));
        assert!(c.is_null(2));

        // test timestamp milliseconds
        let a = TimestampMillisecondArray::from(vec![Some(86405000), Some(1000), None])
            .with_timezone("+01:00".to_string());
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Time32(TimeUnit::Second)).unwrap();
        let c = b.as_any().downcast_ref::<Time32SecondArray>().unwrap();
        assert_eq!(3605, c.value(0));
        assert_eq!(3601, c.value(1));
        assert!(c.is_null(2));
        let b = cast(&array, &DataType::Time32(TimeUnit::Millisecond)).unwrap();
        let c = b.as_any().downcast_ref::<Time32MillisecondArray>().unwrap();
        assert_eq!(3605000, c.value(0));
        assert_eq!(3601000, c.value(1));
        assert!(c.is_null(2));

        // test timestamp microseconds
        let a =
            TimestampMicrosecondArray::from(vec![Some(86405000000), Some(1000000), None])
                .with_timezone("+01:00".to_string());
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Time32(TimeUnit::Second)).unwrap();
        let c = b.as_any().downcast_ref::<Time32SecondArray>().unwrap();
        assert_eq!(3605, c.value(0));
        assert_eq!(3601, c.value(1));
        assert!(c.is_null(2));
        let b = cast(&array, &DataType::Time32(TimeUnit::Millisecond)).unwrap();
        let c = b.as_any().downcast_ref::<Time32MillisecondArray>().unwrap();
        assert_eq!(3605000, c.value(0));
        assert_eq!(3601000, c.value(1));
        assert!(c.is_null(2));

        // test timestamp nanoseconds
        let a = TimestampNanosecondArray::from(vec![
            Some(86405000000000),
            Some(1000000000),
            None,
        ])
        .with_timezone("+01:00".to_string());
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Time32(TimeUnit::Second)).unwrap();
        let c = b.as_any().downcast_ref::<Time32SecondArray>().unwrap();
        assert_eq!(3605, c.value(0));
        assert_eq!(3601, c.value(1));
        assert!(c.is_null(2));
        let b = cast(&array, &DataType::Time32(TimeUnit::Millisecond)).unwrap();
        let c = b.as_any().downcast_ref::<Time32MillisecondArray>().unwrap();
        assert_eq!(3605000, c.value(0));
        assert_eq!(3601000, c.value(1));
        assert!(c.is_null(2));

        // test overflow
        let a = TimestampSecondArray::from(vec![Some(i64::MAX)])
            .with_timezone("+01:00".to_string());
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Time32(TimeUnit::Second));
        assert!(b.is_err());
        let b = cast(&array, &DataType::Time32(TimeUnit::Millisecond));
        assert!(b.is_err());
    }

    #[test]
    fn test_cast_date64_to_timestamp() {
        let array =
            Date64Array::from(vec![Some(864000000005), Some(1545696000001), None]);
        let b = cast(&array, &DataType::Timestamp(TimeUnit::Second, None)).unwrap();
        let c = b.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
        assert_eq!(864000000, c.value(0));
        assert_eq!(1545696000, c.value(1));
        assert!(c.is_null(2));
    }

    #[test]
    fn test_cast_date64_to_timestamp_ms() {
        let array =
            Date64Array::from(vec![Some(864000000005), Some(1545696000001), None]);
        let b = cast(&array, &DataType::Timestamp(TimeUnit::Millisecond, None)).unwrap();
        let c = b
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(864000000005, c.value(0));
        assert_eq!(1545696000001, c.value(1));
        assert!(c.is_null(2));
    }

    #[test]
    fn test_cast_date64_to_timestamp_us() {
        let array =
            Date64Array::from(vec![Some(864000000005), Some(1545696000001), None]);
        let b = cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();
        let c = b
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(864000000005000, c.value(0));
        assert_eq!(1545696000001000, c.value(1));
        assert!(c.is_null(2));
    }

    #[test]
    fn test_cast_date64_to_timestamp_ns() {
        let array =
            Date64Array::from(vec![Some(864000000005), Some(1545696000001), None]);
        let b = cast(&array, &DataType::Timestamp(TimeUnit::Nanosecond, None)).unwrap();
        let c = b
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(864000000005000000, c.value(0));
        assert_eq!(1545696000001000000, c.value(1));
        assert!(c.is_null(2));
    }

    #[test]
    fn test_cast_timestamp_to_i64() {
        let array = TimestampMillisecondArray::from(vec![
            Some(864000000005),
            Some(1545696000001),
            None,
        ])
        .with_timezone("UTC".to_string());
        let b = cast(&array, &DataType::Int64).unwrap();
        let c = b.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(&DataType::Int64, c.data_type());
        assert_eq!(864000000005, c.value(0));
        assert_eq!(1545696000001, c.value(1));
        assert!(c.is_null(2));
    }

    #[test]
    fn test_cast_date32_to_string() {
        let array = Date32Array::from(vec![10000, 17890]);
        let b = cast(&array, &DataType::Utf8).unwrap();
        let c = b.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(&DataType::Utf8, c.data_type());
        assert_eq!("1997-05-19", c.value(0));
        assert_eq!("2018-12-25", c.value(1));
    }

    #[test]
    fn test_cast_date64_to_string() {
        let array = Date64Array::from(vec![10000 * 86400000, 17890 * 86400000]);
        let b = cast(&array, &DataType::Utf8).unwrap();
        let c = b.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(&DataType::Utf8, c.data_type());
        assert_eq!("1997-05-19T00:00:00", c.value(0));
        assert_eq!("2018-12-25T00:00:00", c.value(1));
    }

    #[test]
    fn test_cast_between_timestamps() {
        let array = TimestampMillisecondArray::from(vec![
            Some(864000003005),
            Some(1545696002001),
            None,
        ]);
        let b = cast(&array, &DataType::Timestamp(TimeUnit::Second, None)).unwrap();
        let c = b.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
        assert_eq!(864000003, c.value(0));
        assert_eq!(1545696002, c.value(1));
        assert!(c.is_null(2));
    }

    #[test]
    fn test_cast_duration_to_i64() {
        let base = vec![5, 6, 7, 8, 100000000];

        let duration_arrays = vec![
            Arc::new(DurationNanosecondArray::from(base.clone())) as ArrayRef,
            Arc::new(DurationMicrosecondArray::from(base.clone())) as ArrayRef,
            Arc::new(DurationMillisecondArray::from(base.clone())) as ArrayRef,
            Arc::new(DurationSecondArray::from(base.clone())) as ArrayRef,
        ];

        for arr in duration_arrays {
            assert!(can_cast_types(arr.data_type(), &DataType::Int64));
            let result = cast(&arr, &DataType::Int64).unwrap();
            let result = result.as_any().downcast_ref::<Int64Array>().unwrap();
            assert_eq!(base.as_slice(), result.values());
        }
    }

    #[test]
    fn test_cast_interval_to_i64() {
        let base = vec![5, 6, 7, 8];

        let interval_arrays = vec![
            Arc::new(IntervalDayTimeArray::from(base.clone())) as ArrayRef,
            Arc::new(IntervalYearMonthArray::from(
                base.iter().map(|x| *x as i32).collect::<Vec<i32>>(),
            )) as ArrayRef,
        ];

        for arr in interval_arrays {
            assert!(can_cast_types(arr.data_type(), &DataType::Int64));
            let result = cast(&arr, &DataType::Int64).unwrap();
            let result = result.as_any().downcast_ref::<Int64Array>().unwrap();
            assert_eq!(base.as_slice(), result.values());
        }
    }

    #[test]
    fn test_cast_to_strings() {
        let a = Int32Array::from(vec![1, 2, 3]);
        let out = cast(&a, &DataType::Utf8).unwrap();
        let out = out
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(out, vec![Some("1"), Some("2"), Some("3")]);
        let out = cast(&a, &DataType::LargeUtf8).unwrap();
        let out = out
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(out, vec![Some("1"), Some("2"), Some("3")]);
    }

    #[test]
    fn test_str_to_str_casts() {
        for data in vec![
            vec![Some("foo"), Some("bar"), Some("ham")],
            vec![Some("foo"), None, Some("bar")],
        ] {
            let a = LargeStringArray::from(data.clone());
            let to = cast(&a, &DataType::Utf8).unwrap();
            let expect = a
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>();
            let out = to
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>();
            assert_eq!(expect, out);

            let a = StringArray::from(data);
            let to = cast(&a, &DataType::LargeUtf8).unwrap();
            let expect = a
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>();
            let out = to
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>();
            assert_eq!(expect, out);
        }
    }

    #[test]
    fn test_cast_from_f64() {
        let f64_values: Vec<f64> = vec![
            i64::MIN as f64,
            i32::MIN as f64,
            i16::MIN as f64,
            i8::MIN as f64,
            0_f64,
            u8::MAX as f64,
            u16::MAX as f64,
            u32::MAX as f64,
            u64::MAX as f64,
        ];
        let f64_array: ArrayRef = Arc::new(Float64Array::from(f64_values));

        let f64_expected = vec![
            -9223372036854776000.0,
            -2147483648.0,
            -32768.0,
            -128.0,
            0.0,
            255.0,
            65535.0,
            4294967295.0,
            18446744073709552000.0,
        ];
        assert_eq!(
            f64_expected,
            get_cast_values::<Float64Type>(&f64_array, &DataType::Float64)
                .iter()
                .map(|i| i.parse::<f64>().unwrap())
                .collect::<Vec<f64>>()
        );

        let f32_expected = vec![
            -9223372000000000000.0,
            -2147483600.0,
            -32768.0,
            -128.0,
            0.0,
            255.0,
            65535.0,
            4294967300.0,
            18446744000000000000.0,
        ];
        assert_eq!(
            f32_expected,
            get_cast_values::<Float32Type>(&f64_array, &DataType::Float32)
                .iter()
                .map(|i| i.parse::<f32>().unwrap())
                .collect::<Vec<f32>>()
        );

        let i64_expected = vec![
            "-9223372036854775808",
            "-2147483648",
            "-32768",
            "-128",
            "0",
            "255",
            "65535",
            "4294967295",
            "null",
        ];
        assert_eq!(
            i64_expected,
            get_cast_values::<Int64Type>(&f64_array, &DataType::Int64)
        );

        let i32_expected = vec![
            "null",
            "-2147483648",
            "-32768",
            "-128",
            "0",
            "255",
            "65535",
            "null",
            "null",
        ];
        assert_eq!(
            i32_expected,
            get_cast_values::<Int32Type>(&f64_array, &DataType::Int32)
        );

        let i16_expected = vec![
            "null", "null", "-32768", "-128", "0", "255", "null", "null", "null",
        ];
        assert_eq!(
            i16_expected,
            get_cast_values::<Int16Type>(&f64_array, &DataType::Int16)
        );

        let i8_expected = vec![
            "null", "null", "null", "-128", "0", "null", "null", "null", "null",
        ];
        assert_eq!(
            i8_expected,
            get_cast_values::<Int8Type>(&f64_array, &DataType::Int8)
        );

        let u64_expected = vec![
            "null",
            "null",
            "null",
            "null",
            "0",
            "255",
            "65535",
            "4294967295",
            "null",
        ];
        assert_eq!(
            u64_expected,
            get_cast_values::<UInt64Type>(&f64_array, &DataType::UInt64)
        );

        let u32_expected = vec![
            "null",
            "null",
            "null",
            "null",
            "0",
            "255",
            "65535",
            "4294967295",
            "null",
        ];
        assert_eq!(
            u32_expected,
            get_cast_values::<UInt32Type>(&f64_array, &DataType::UInt32)
        );

        let u16_expected = vec![
            "null", "null", "null", "null", "0", "255", "65535", "null", "null",
        ];
        assert_eq!(
            u16_expected,
            get_cast_values::<UInt16Type>(&f64_array, &DataType::UInt16)
        );

        let u8_expected = vec![
            "null", "null", "null", "null", "0", "255", "null", "null", "null",
        ];
        assert_eq!(
            u8_expected,
            get_cast_values::<UInt8Type>(&f64_array, &DataType::UInt8)
        );
    }

    #[test]
    fn test_cast_from_f32() {
        let f32_values: Vec<f32> = vec![
            i32::MIN as f32,
            i32::MIN as f32,
            i16::MIN as f32,
            i8::MIN as f32,
            0_f32,
            u8::MAX as f32,
            u16::MAX as f32,
            u32::MAX as f32,
            u32::MAX as f32,
        ];
        let f32_array: ArrayRef = Arc::new(Float32Array::from(f32_values));

        let f64_expected = vec![
            "-2147483648.0",
            "-2147483648.0",
            "-32768.0",
            "-128.0",
            "0.0",
            "255.0",
            "65535.0",
            "4294967296.0",
            "4294967296.0",
        ];
        assert_eq!(
            f64_expected,
            get_cast_values::<Float64Type>(&f32_array, &DataType::Float64)
        );

        let f32_expected = vec![
            "-2147483600.0",
            "-2147483600.0",
            "-32768.0",
            "-128.0",
            "0.0",
            "255.0",
            "65535.0",
            "4294967300.0",
            "4294967300.0",
        ];
        assert_eq!(
            f32_expected,
            get_cast_values::<Float32Type>(&f32_array, &DataType::Float32)
        );

        let i64_expected = vec![
            "-2147483648",
            "-2147483648",
            "-32768",
            "-128",
            "0",
            "255",
            "65535",
            "4294967296",
            "4294967296",
        ];
        assert_eq!(
            i64_expected,
            get_cast_values::<Int64Type>(&f32_array, &DataType::Int64)
        );

        let i32_expected = vec![
            "-2147483648",
            "-2147483648",
            "-32768",
            "-128",
            "0",
            "255",
            "65535",
            "null",
            "null",
        ];
        assert_eq!(
            i32_expected,
            get_cast_values::<Int32Type>(&f32_array, &DataType::Int32)
        );

        let i16_expected = vec![
            "null", "null", "-32768", "-128", "0", "255", "null", "null", "null",
        ];
        assert_eq!(
            i16_expected,
            get_cast_values::<Int16Type>(&f32_array, &DataType::Int16)
        );

        let i8_expected = vec![
            "null", "null", "null", "-128", "0", "null", "null", "null", "null",
        ];
        assert_eq!(
            i8_expected,
            get_cast_values::<Int8Type>(&f32_array, &DataType::Int8)
        );

        let u64_expected = vec![
            "null",
            "null",
            "null",
            "null",
            "0",
            "255",
            "65535",
            "4294967296",
            "4294967296",
        ];
        assert_eq!(
            u64_expected,
            get_cast_values::<UInt64Type>(&f32_array, &DataType::UInt64)
        );

        let u32_expected = vec![
            "null", "null", "null", "null", "0", "255", "65535", "null", "null",
        ];
        assert_eq!(
            u32_expected,
            get_cast_values::<UInt32Type>(&f32_array, &DataType::UInt32)
        );

        let u16_expected = vec![
            "null", "null", "null", "null", "0", "255", "65535", "null", "null",
        ];
        assert_eq!(
            u16_expected,
            get_cast_values::<UInt16Type>(&f32_array, &DataType::UInt16)
        );

        let u8_expected = vec![
            "null", "null", "null", "null", "0", "255", "null", "null", "null",
        ];
        assert_eq!(
            u8_expected,
            get_cast_values::<UInt8Type>(&f32_array, &DataType::UInt8)
        );
    }

    #[test]
    fn test_cast_from_uint64() {
        let u64_values: Vec<u64> = vec![
            0,
            u8::MAX as u64,
            u16::MAX as u64,
            u32::MAX as u64,
            u64::MAX,
        ];
        let u64_array: ArrayRef = Arc::new(UInt64Array::from(u64_values));

        let f64_expected =
            vec![0.0, 255.0, 65535.0, 4294967295.0, 18446744073709552000.0];
        assert_eq!(
            f64_expected,
            get_cast_values::<Float64Type>(&u64_array, &DataType::Float64)
                .iter()
                .map(|i| i.parse::<f64>().unwrap())
                .collect::<Vec<f64>>()
        );

        let f32_expected =
            vec![0.0, 255.0, 65535.0, 4294967300.0, 18446744000000000000.0];
        assert_eq!(
            f32_expected,
            get_cast_values::<Float32Type>(&u64_array, &DataType::Float32)
                .iter()
                .map(|i| i.parse::<f32>().unwrap())
                .collect::<Vec<f32>>()
        );

        let i64_expected = vec!["0", "255", "65535", "4294967295", "null"];
        assert_eq!(
            i64_expected,
            get_cast_values::<Int64Type>(&u64_array, &DataType::Int64)
        );

        let i32_expected = vec!["0", "255", "65535", "null", "null"];
        assert_eq!(
            i32_expected,
            get_cast_values::<Int32Type>(&u64_array, &DataType::Int32)
        );

        let i16_expected = vec!["0", "255", "null", "null", "null"];
        assert_eq!(
            i16_expected,
            get_cast_values::<Int16Type>(&u64_array, &DataType::Int16)
        );

        let i8_expected = vec!["0", "null", "null", "null", "null"];
        assert_eq!(
            i8_expected,
            get_cast_values::<Int8Type>(&u64_array, &DataType::Int8)
        );

        let u64_expected =
            vec!["0", "255", "65535", "4294967295", "18446744073709551615"];
        assert_eq!(
            u64_expected,
            get_cast_values::<UInt64Type>(&u64_array, &DataType::UInt64)
        );

        let u32_expected = vec!["0", "255", "65535", "4294967295", "null"];
        assert_eq!(
            u32_expected,
            get_cast_values::<UInt32Type>(&u64_array, &DataType::UInt32)
        );

        let u16_expected = vec!["0", "255", "65535", "null", "null"];
        assert_eq!(
            u16_expected,
            get_cast_values::<UInt16Type>(&u64_array, &DataType::UInt16)
        );

        let u8_expected = vec!["0", "255", "null", "null", "null"];
        assert_eq!(
            u8_expected,
            get_cast_values::<UInt8Type>(&u64_array, &DataType::UInt8)
        );
    }

    #[test]
    fn test_cast_from_uint32() {
        let u32_values: Vec<u32> = vec![0, u8::MAX as u32, u16::MAX as u32, u32::MAX];
        let u32_array: ArrayRef = Arc::new(UInt32Array::from(u32_values));

        let f64_expected = vec!["0.0", "255.0", "65535.0", "4294967295.0"];
        assert_eq!(
            f64_expected,
            get_cast_values::<Float64Type>(&u32_array, &DataType::Float64)
        );

        let f32_expected = vec!["0.0", "255.0", "65535.0", "4294967300.0"];
        assert_eq!(
            f32_expected,
            get_cast_values::<Float32Type>(&u32_array, &DataType::Float32)
        );

        let i64_expected = vec!["0", "255", "65535", "4294967295"];
        assert_eq!(
            i64_expected,
            get_cast_values::<Int64Type>(&u32_array, &DataType::Int64)
        );

        let i32_expected = vec!["0", "255", "65535", "null"];
        assert_eq!(
            i32_expected,
            get_cast_values::<Int32Type>(&u32_array, &DataType::Int32)
        );

        let i16_expected = vec!["0", "255", "null", "null"];
        assert_eq!(
            i16_expected,
            get_cast_values::<Int16Type>(&u32_array, &DataType::Int16)
        );

        let i8_expected = vec!["0", "null", "null", "null"];
        assert_eq!(
            i8_expected,
            get_cast_values::<Int8Type>(&u32_array, &DataType::Int8)
        );

        let u64_expected = vec!["0", "255", "65535", "4294967295"];
        assert_eq!(
            u64_expected,
            get_cast_values::<UInt64Type>(&u32_array, &DataType::UInt64)
        );

        let u32_expected = vec!["0", "255", "65535", "4294967295"];
        assert_eq!(
            u32_expected,
            get_cast_values::<UInt32Type>(&u32_array, &DataType::UInt32)
        );

        let u16_expected = vec!["0", "255", "65535", "null"];
        assert_eq!(
            u16_expected,
            get_cast_values::<UInt16Type>(&u32_array, &DataType::UInt16)
        );

        let u8_expected = vec!["0", "255", "null", "null"];
        assert_eq!(
            u8_expected,
            get_cast_values::<UInt8Type>(&u32_array, &DataType::UInt8)
        );
    }

    #[test]
    fn test_cast_from_uint16() {
        let u16_values: Vec<u16> = vec![0, u8::MAX as u16, u16::MAX];
        let u16_array: ArrayRef = Arc::new(UInt16Array::from(u16_values));

        let f64_expected = vec!["0.0", "255.0", "65535.0"];
        assert_eq!(
            f64_expected,
            get_cast_values::<Float64Type>(&u16_array, &DataType::Float64)
        );

        let f32_expected = vec!["0.0", "255.0", "65535.0"];
        assert_eq!(
            f32_expected,
            get_cast_values::<Float32Type>(&u16_array, &DataType::Float32)
        );

        let i64_expected = vec!["0", "255", "65535"];
        assert_eq!(
            i64_expected,
            get_cast_values::<Int64Type>(&u16_array, &DataType::Int64)
        );

        let i32_expected = vec!["0", "255", "65535"];
        assert_eq!(
            i32_expected,
            get_cast_values::<Int32Type>(&u16_array, &DataType::Int32)
        );

        let i16_expected = vec!["0", "255", "null"];
        assert_eq!(
            i16_expected,
            get_cast_values::<Int16Type>(&u16_array, &DataType::Int16)
        );

        let i8_expected = vec!["0", "null", "null"];
        assert_eq!(
            i8_expected,
            get_cast_values::<Int8Type>(&u16_array, &DataType::Int8)
        );

        let u64_expected = vec!["0", "255", "65535"];
        assert_eq!(
            u64_expected,
            get_cast_values::<UInt64Type>(&u16_array, &DataType::UInt64)
        );

        let u32_expected = vec!["0", "255", "65535"];
        assert_eq!(
            u32_expected,
            get_cast_values::<UInt32Type>(&u16_array, &DataType::UInt32)
        );

        let u16_expected = vec!["0", "255", "65535"];
        assert_eq!(
            u16_expected,
            get_cast_values::<UInt16Type>(&u16_array, &DataType::UInt16)
        );

        let u8_expected = vec!["0", "255", "null"];
        assert_eq!(
            u8_expected,
            get_cast_values::<UInt8Type>(&u16_array, &DataType::UInt8)
        );
    }

    #[test]
    fn test_cast_from_uint8() {
        let u8_values: Vec<u8> = vec![0, u8::MAX];
        let u8_array: ArrayRef = Arc::new(UInt8Array::from(u8_values));

        let f64_expected = vec!["0.0", "255.0"];
        assert_eq!(
            f64_expected,
            get_cast_values::<Float64Type>(&u8_array, &DataType::Float64)
        );

        let f32_expected = vec!["0.0", "255.0"];
        assert_eq!(
            f32_expected,
            get_cast_values::<Float32Type>(&u8_array, &DataType::Float32)
        );

        let i64_expected = vec!["0", "255"];
        assert_eq!(
            i64_expected,
            get_cast_values::<Int64Type>(&u8_array, &DataType::Int64)
        );

        let i32_expected = vec!["0", "255"];
        assert_eq!(
            i32_expected,
            get_cast_values::<Int32Type>(&u8_array, &DataType::Int32)
        );

        let i16_expected = vec!["0", "255"];
        assert_eq!(
            i16_expected,
            get_cast_values::<Int16Type>(&u8_array, &DataType::Int16)
        );

        let i8_expected = vec!["0", "null"];
        assert_eq!(
            i8_expected,
            get_cast_values::<Int8Type>(&u8_array, &DataType::Int8)
        );

        let u64_expected = vec!["0", "255"];
        assert_eq!(
            u64_expected,
            get_cast_values::<UInt64Type>(&u8_array, &DataType::UInt64)
        );

        let u32_expected = vec!["0", "255"];
        assert_eq!(
            u32_expected,
            get_cast_values::<UInt32Type>(&u8_array, &DataType::UInt32)
        );

        let u16_expected = vec!["0", "255"];
        assert_eq!(
            u16_expected,
            get_cast_values::<UInt16Type>(&u8_array, &DataType::UInt16)
        );

        let u8_expected = vec!["0", "255"];
        assert_eq!(
            u8_expected,
            get_cast_values::<UInt8Type>(&u8_array, &DataType::UInt8)
        );
    }

    #[test]
    fn test_cast_from_int64() {
        let i64_values: Vec<i64> = vec![
            i64::MIN,
            i32::MIN as i64,
            i16::MIN as i64,
            i8::MIN as i64,
            0,
            i8::MAX as i64,
            i16::MAX as i64,
            i32::MAX as i64,
            i64::MAX,
        ];
        let i64_array: ArrayRef = Arc::new(Int64Array::from(i64_values));

        let f64_expected = vec![
            -9223372036854776000.0,
            -2147483648.0,
            -32768.0,
            -128.0,
            0.0,
            127.0,
            32767.0,
            2147483647.0,
            9223372036854776000.0,
        ];
        assert_eq!(
            f64_expected,
            get_cast_values::<Float64Type>(&i64_array, &DataType::Float64)
                .iter()
                .map(|i| i.parse::<f64>().unwrap())
                .collect::<Vec<f64>>()
        );

        let f32_expected = vec![
            -9223372000000000000.0,
            -2147483600.0,
            -32768.0,
            -128.0,
            0.0,
            127.0,
            32767.0,
            2147483600.0,
            9223372000000000000.0,
        ];
        assert_eq!(
            f32_expected,
            get_cast_values::<Float32Type>(&i64_array, &DataType::Float32)
                .iter()
                .map(|i| i.parse::<f32>().unwrap())
                .collect::<Vec<f32>>()
        );

        let i64_expected = vec![
            "-9223372036854775808",
            "-2147483648",
            "-32768",
            "-128",
            "0",
            "127",
            "32767",
            "2147483647",
            "9223372036854775807",
        ];
        assert_eq!(
            i64_expected,
            get_cast_values::<Int64Type>(&i64_array, &DataType::Int64)
        );

        let i32_expected = vec![
            "null",
            "-2147483648",
            "-32768",
            "-128",
            "0",
            "127",
            "32767",
            "2147483647",
            "null",
        ];
        assert_eq!(
            i32_expected,
            get_cast_values::<Int32Type>(&i64_array, &DataType::Int32)
        );

        assert_eq!(
            i32_expected,
            get_cast_values::<Date32Type>(&i64_array, &DataType::Date32)
        );

        let i16_expected = vec![
            "null", "null", "-32768", "-128", "0", "127", "32767", "null", "null",
        ];
        assert_eq!(
            i16_expected,
            get_cast_values::<Int16Type>(&i64_array, &DataType::Int16)
        );

        let i8_expected = vec![
            "null", "null", "null", "-128", "0", "127", "null", "null", "null",
        ];
        assert_eq!(
            i8_expected,
            get_cast_values::<Int8Type>(&i64_array, &DataType::Int8)
        );

        let u64_expected = vec![
            "null",
            "null",
            "null",
            "null",
            "0",
            "127",
            "32767",
            "2147483647",
            "9223372036854775807",
        ];
        assert_eq!(
            u64_expected,
            get_cast_values::<UInt64Type>(&i64_array, &DataType::UInt64)
        );

        let u32_expected = vec![
            "null",
            "null",
            "null",
            "null",
            "0",
            "127",
            "32767",
            "2147483647",
            "null",
        ];
        assert_eq!(
            u32_expected,
            get_cast_values::<UInt32Type>(&i64_array, &DataType::UInt32)
        );

        let u16_expected = vec![
            "null", "null", "null", "null", "0", "127", "32767", "null", "null",
        ];
        assert_eq!(
            u16_expected,
            get_cast_values::<UInt16Type>(&i64_array, &DataType::UInt16)
        );

        let u8_expected = vec![
            "null", "null", "null", "null", "0", "127", "null", "null", "null",
        ];
        assert_eq!(
            u8_expected,
            get_cast_values::<UInt8Type>(&i64_array, &DataType::UInt8)
        );
    }

    #[test]
    fn test_cast_from_int32() {
        let i32_values: Vec<i32> = vec![
            i32::MIN,
            i16::MIN as i32,
            i8::MIN as i32,
            0,
            i8::MAX as i32,
            i16::MAX as i32,
            i32::MAX,
        ];
        let i32_array: ArrayRef = Arc::new(Int32Array::from(i32_values));

        let f64_expected = vec![
            "-2147483648.0",
            "-32768.0",
            "-128.0",
            "0.0",
            "127.0",
            "32767.0",
            "2147483647.0",
        ];
        assert_eq!(
            f64_expected,
            get_cast_values::<Float64Type>(&i32_array, &DataType::Float64)
        );

        let f32_expected = vec![
            "-2147483600.0",
            "-32768.0",
            "-128.0",
            "0.0",
            "127.0",
            "32767.0",
            "2147483600.0",
        ];
        assert_eq!(
            f32_expected,
            get_cast_values::<Float32Type>(&i32_array, &DataType::Float32)
        );

        let i16_expected = vec!["null", "-32768", "-128", "0", "127", "32767", "null"];
        assert_eq!(
            i16_expected,
            get_cast_values::<Int16Type>(&i32_array, &DataType::Int16)
        );

        let i8_expected = vec!["null", "null", "-128", "0", "127", "null", "null"];
        assert_eq!(
            i8_expected,
            get_cast_values::<Int8Type>(&i32_array, &DataType::Int8)
        );

        let u64_expected =
            vec!["null", "null", "null", "0", "127", "32767", "2147483647"];
        assert_eq!(
            u64_expected,
            get_cast_values::<UInt64Type>(&i32_array, &DataType::UInt64)
        );

        let u32_expected =
            vec!["null", "null", "null", "0", "127", "32767", "2147483647"];
        assert_eq!(
            u32_expected,
            get_cast_values::<UInt32Type>(&i32_array, &DataType::UInt32)
        );

        let u16_expected = vec!["null", "null", "null", "0", "127", "32767", "null"];
        assert_eq!(
            u16_expected,
            get_cast_values::<UInt16Type>(&i32_array, &DataType::UInt16)
        );

        let u8_expected = vec!["null", "null", "null", "0", "127", "null", "null"];
        assert_eq!(
            u8_expected,
            get_cast_values::<UInt8Type>(&i32_array, &DataType::UInt8)
        );

        // The date32 to date64 cast increases the numerical values in order to keep the same dates.
        let i64_expected = vec![
            "-185542587187200000",
            "-2831155200000",
            "-11059200000",
            "0",
            "10972800000",
            "2831068800000",
            "185542587100800000",
        ];
        assert_eq!(
            i64_expected,
            get_cast_values::<Date64Type>(&i32_array, &DataType::Date64)
        );
    }

    #[test]
    fn test_cast_from_int16() {
        let i16_values: Vec<i16> =
            vec![i16::MIN, i8::MIN as i16, 0, i8::MAX as i16, i16::MAX];
        let i16_array: ArrayRef = Arc::new(Int16Array::from(i16_values));

        let f64_expected = vec!["-32768.0", "-128.0", "0.0", "127.0", "32767.0"];
        assert_eq!(
            f64_expected,
            get_cast_values::<Float64Type>(&i16_array, &DataType::Float64)
        );

        let f32_expected = vec!["-32768.0", "-128.0", "0.0", "127.0", "32767.0"];
        assert_eq!(
            f32_expected,
            get_cast_values::<Float32Type>(&i16_array, &DataType::Float32)
        );

        let i64_expected = vec!["-32768", "-128", "0", "127", "32767"];
        assert_eq!(
            i64_expected,
            get_cast_values::<Int64Type>(&i16_array, &DataType::Int64)
        );

        let i32_expected = vec!["-32768", "-128", "0", "127", "32767"];
        assert_eq!(
            i32_expected,
            get_cast_values::<Int32Type>(&i16_array, &DataType::Int32)
        );

        let i16_expected = vec!["-32768", "-128", "0", "127", "32767"];
        assert_eq!(
            i16_expected,
            get_cast_values::<Int16Type>(&i16_array, &DataType::Int16)
        );

        let i8_expected = vec!["null", "-128", "0", "127", "null"];
        assert_eq!(
            i8_expected,
            get_cast_values::<Int8Type>(&i16_array, &DataType::Int8)
        );

        let u64_expected = vec!["null", "null", "0", "127", "32767"];
        assert_eq!(
            u64_expected,
            get_cast_values::<UInt64Type>(&i16_array, &DataType::UInt64)
        );

        let u32_expected = vec!["null", "null", "0", "127", "32767"];
        assert_eq!(
            u32_expected,
            get_cast_values::<UInt32Type>(&i16_array, &DataType::UInt32)
        );

        let u16_expected = vec!["null", "null", "0", "127", "32767"];
        assert_eq!(
            u16_expected,
            get_cast_values::<UInt16Type>(&i16_array, &DataType::UInt16)
        );

        let u8_expected = vec!["null", "null", "0", "127", "null"];
        assert_eq!(
            u8_expected,
            get_cast_values::<UInt8Type>(&i16_array, &DataType::UInt8)
        );
    }

    #[test]
    fn test_cast_from_date32() {
        let i32_values: Vec<i32> = vec![
            i32::MIN,
            i16::MIN as i32,
            i8::MIN as i32,
            0,
            i8::MAX as i32,
            i16::MAX as i32,
            i32::MAX,
        ];
        let date32_array: ArrayRef = Arc::new(Date32Array::from(i32_values));

        let i64_expected = vec![
            "-2147483648",
            "-32768",
            "-128",
            "0",
            "127",
            "32767",
            "2147483647",
        ];
        assert_eq!(
            i64_expected,
            get_cast_values::<Int64Type>(&date32_array, &DataType::Int64)
        );
    }

    #[test]
    fn test_cast_from_int8() {
        let i8_values: Vec<i8> = vec![i8::MIN, 0, i8::MAX];
        let i8_array = Int8Array::from(i8_values);

        let f64_expected = vec!["-128.0", "0.0", "127.0"];
        assert_eq!(
            f64_expected,
            get_cast_values::<Float64Type>(&i8_array, &DataType::Float64)
        );

        let f32_expected = vec!["-128.0", "0.0", "127.0"];
        assert_eq!(
            f32_expected,
            get_cast_values::<Float32Type>(&i8_array, &DataType::Float32)
        );

        let i64_expected = vec!["-128", "0", "127"];
        assert_eq!(
            i64_expected,
            get_cast_values::<Int64Type>(&i8_array, &DataType::Int64)
        );

        let i32_expected = vec!["-128", "0", "127"];
        assert_eq!(
            i32_expected,
            get_cast_values::<Int32Type>(&i8_array, &DataType::Int32)
        );

        let i16_expected = vec!["-128", "0", "127"];
        assert_eq!(
            i16_expected,
            get_cast_values::<Int16Type>(&i8_array, &DataType::Int16)
        );

        let i8_expected = vec!["-128", "0", "127"];
        assert_eq!(
            i8_expected,
            get_cast_values::<Int8Type>(&i8_array, &DataType::Int8)
        );

        let u64_expected = vec!["null", "0", "127"];
        assert_eq!(
            u64_expected,
            get_cast_values::<UInt64Type>(&i8_array, &DataType::UInt64)
        );

        let u32_expected = vec!["null", "0", "127"];
        assert_eq!(
            u32_expected,
            get_cast_values::<UInt32Type>(&i8_array, &DataType::UInt32)
        );

        let u16_expected = vec!["null", "0", "127"];
        assert_eq!(
            u16_expected,
            get_cast_values::<UInt16Type>(&i8_array, &DataType::UInt16)
        );

        let u8_expected = vec!["null", "0", "127"];
        assert_eq!(
            u8_expected,
            get_cast_values::<UInt8Type>(&i8_array, &DataType::UInt8)
        );
    }

    /// Convert `array` into a vector of strings by casting to data type dt
    fn get_cast_values<T>(array: &dyn Array, dt: &DataType) -> Vec<String>
    where
        T: ArrowPrimitiveType,
    {
        let c = cast(array, dt).unwrap();
        let a = c.as_primitive::<T>();
        let mut v: Vec<String> = vec![];
        for i in 0..array.len() {
            if a.is_null(i) {
                v.push("null".to_string())
            } else {
                v.push(format!("{:?}", a.value(i)));
            }
        }
        v
    }

    #[test]
    fn test_cast_utf8_dict() {
        // FROM a dictionary with of Utf8 values
        use DataType::*;

        let mut builder = StringDictionaryBuilder::<Int8Type>::new();
        builder.append("one").unwrap();
        builder.append_null();
        builder.append("three").unwrap();
        let array: ArrayRef = Arc::new(builder.finish());

        let expected = vec!["one", "null", "three"];

        // Test casting TO StringArray
        let cast_type = Utf8;
        let cast_array = cast(&array, &cast_type).expect("cast to UTF-8 failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);

        // Test casting TO Dictionary (with different index sizes)

        let cast_type = Dictionary(Box::new(Int16), Box::new(Utf8));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);

        let cast_type = Dictionary(Box::new(Int32), Box::new(Utf8));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);

        let cast_type = Dictionary(Box::new(Int64), Box::new(Utf8));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);

        let cast_type = Dictionary(Box::new(UInt8), Box::new(Utf8));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);

        let cast_type = Dictionary(Box::new(UInt16), Box::new(Utf8));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);

        let cast_type = Dictionary(Box::new(UInt32), Box::new(Utf8));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);

        let cast_type = Dictionary(Box::new(UInt64), Box::new(Utf8));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);
    }

    #[test]
    fn test_cast_dict_to_dict_bad_index_value_primitive() {
        use DataType::*;
        // test converting from an array that has indexes of a type
        // that are out of bounds for a particular other kind of
        // index.

        let mut builder = PrimitiveDictionaryBuilder::<Int32Type, Int64Type>::new();

        // add 200 distinct values (which can be stored by a
        // dictionary indexed by int32, but not a dictionary indexed
        // with int8)
        for i in 0..200 {
            builder.append(i).unwrap();
        }
        let array: ArrayRef = Arc::new(builder.finish());

        let cast_type = Dictionary(Box::new(Int8), Box::new(Utf8));
        let res = cast(&array, &cast_type);
        assert!(res.is_err());
        let actual_error = format!("{res:?}");
        let expected_error = "Could not convert 72 dictionary indexes from Int32 to Int8";
        assert!(
            actual_error.contains(expected_error),
            "did not find expected error '{actual_error}' in actual error '{expected_error}'"
        );
    }

    #[test]
    fn test_cast_dict_to_dict_bad_index_value_utf8() {
        use DataType::*;
        // Same test as test_cast_dict_to_dict_bad_index_value but use
        // string values (and encode the expected behavior here);

        let mut builder = StringDictionaryBuilder::<Int32Type>::new();

        // add 200 distinct values (which can be stored by a
        // dictionary indexed by int32, but not a dictionary indexed
        // with int8)
        for i in 0..200 {
            let val = format!("val{i}");
            builder.append(&val).unwrap();
        }
        let array = builder.finish();

        let cast_type = Dictionary(Box::new(Int8), Box::new(Utf8));
        let res = cast(&array, &cast_type);
        assert!(res.is_err());
        let actual_error = format!("{res:?}");
        let expected_error = "Could not convert 72 dictionary indexes from Int32 to Int8";
        assert!(
            actual_error.contains(expected_error),
            "did not find expected error '{actual_error}' in actual error '{expected_error}'"
        );
    }

    #[test]
    fn test_cast_primitive_dict() {
        // FROM a dictionary with of INT32 values
        use DataType::*;

        let mut builder = PrimitiveDictionaryBuilder::<Int8Type, Int32Type>::new();
        builder.append(1).unwrap();
        builder.append_null();
        builder.append(3).unwrap();
        let array: ArrayRef = Arc::new(builder.finish());

        let expected = vec!["1", "null", "3"];

        // Test casting TO PrimitiveArray, different dictionary type
        let cast_array = cast(&array, &Utf8).expect("cast to UTF-8 failed");
        assert_eq!(array_to_strings(&cast_array), expected);
        assert_eq!(cast_array.data_type(), &Utf8);

        let cast_array = cast(&array, &Int64).expect("cast to int64 failed");
        assert_eq!(array_to_strings(&cast_array), expected);
        assert_eq!(cast_array.data_type(), &Int64);
    }

    #[test]
    fn test_cast_primitive_array_to_dict() {
        use DataType::*;

        let mut builder = PrimitiveBuilder::<Int32Type>::new();
        builder.append_value(1);
        builder.append_null();
        builder.append_value(3);
        let array: ArrayRef = Arc::new(builder.finish());

        let expected = vec!["1", "null", "3"];

        // Cast to a dictionary (same value type, Int32)
        let cast_type = Dictionary(Box::new(UInt8), Box::new(Int32));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);

        // Cast to a dictionary (different value type, Int8)
        let cast_type = Dictionary(Box::new(UInt8), Box::new(Int8));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);
    }

    #[test]
    fn test_cast_string_array_to_dict() {
        use DataType::*;

        let array = Arc::new(StringArray::from(vec![Some("one"), None, Some("three")]))
            as ArrayRef;

        let expected = vec!["one", "null", "three"];

        // Cast to a dictionary (same value type, Utf8)
        let cast_type = Dictionary(Box::new(UInt8), Box::new(Utf8));
        let cast_array = cast(&array, &cast_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &cast_type);
        assert_eq!(array_to_strings(&cast_array), expected);
    }

    #[test]
    fn test_cast_null_array_to_from_decimal_array() {
        let data_type = DataType::Decimal128(12, 4);
        let array = new_null_array(&DataType::Null, 4);
        assert_eq!(array.data_type(), &DataType::Null);
        let cast_array = cast(&array, &data_type).expect("cast failed");
        assert_eq!(cast_array.data_type(), &data_type);
        for i in 0..4 {
            assert!(cast_array.is_null(i));
        }

        let array = new_null_array(&data_type, 4);
        assert_eq!(array.data_type(), &data_type);
        let cast_array = cast(&array, &DataType::Null).expect("cast failed");
        assert_eq!(cast_array.data_type(), &DataType::Null);
        for i in 0..4 {
            assert!(cast_array.is_null(i));
        }
    }

    #[test]
    fn test_cast_null_array_from_and_to_primitive_array() {
        macro_rules! typed_test {
            ($ARR_TYPE:ident, $DATATYPE:ident, $TYPE:tt) => {{
                {
                    let array = Arc::new(NullArray::new(6)) as ArrayRef;
                    let expected = $ARR_TYPE::from(vec![None; 6]);
                    let cast_type = DataType::$DATATYPE;
                    let cast_array = cast(&array, &cast_type).expect("cast failed");
                    let cast_array = cast_array.as_primitive::<$TYPE>();
                    assert_eq!(cast_array.data_type(), &cast_type);
                    assert_eq!(cast_array, &expected);
                }
            }};
        }

        typed_test!(Int16Array, Int16, Int16Type);
        typed_test!(Int32Array, Int32, Int32Type);
        typed_test!(Int64Array, Int64, Int64Type);

        typed_test!(UInt16Array, UInt16, UInt16Type);
        typed_test!(UInt32Array, UInt32, UInt32Type);
        typed_test!(UInt64Array, UInt64, UInt64Type);

        typed_test!(Float32Array, Float32, Float32Type);
        typed_test!(Float64Array, Float64, Float64Type);

        typed_test!(Date32Array, Date32, Date32Type);
        typed_test!(Date64Array, Date64, Date64Type);
    }

    fn cast_from_null_to_other(data_type: &DataType) {
        // Cast from null to data_type
        {
            let array = new_null_array(&DataType::Null, 4);
            assert_eq!(array.data_type(), &DataType::Null);
            let cast_array = cast(&array, data_type).expect("cast failed");
            assert_eq!(cast_array.data_type(), data_type);
            for i in 0..4 {
                assert!(cast_array.is_null(i));
            }
        }
    }

    #[test]
    fn test_cast_null_from_and_to_variable_sized() {
        cast_from_null_to_other(&DataType::Utf8);
        cast_from_null_to_other(&DataType::LargeUtf8);
        cast_from_null_to_other(&DataType::Binary);
        cast_from_null_to_other(&DataType::LargeBinary);
    }

    #[test]
    fn test_cast_null_from_and_to_nested_type() {
        // Cast null from and to map
        let data_type = DataType::Map(
            Arc::new(Field::new_struct(
                "entry",
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Int32, true),
                ],
                false,
            )),
            false,
        );
        cast_from_null_to_other(&data_type);

        // Cast null from and to list
        let data_type =
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        cast_from_null_to_other(&data_type);
        let data_type =
            DataType::LargeList(Arc::new(Field::new("item", DataType::Int32, true)));
        cast_from_null_to_other(&data_type);
        let data_type = DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Int32, true)),
            4,
        );
        cast_from_null_to_other(&data_type);

        // Cast null from and to dictionary
        let values = vec![None, None, None, None] as Vec<Option<&str>>;
        let array: DictionaryArray<Int8Type> = values.into_iter().collect();
        let array = Arc::new(array) as ArrayRef;
        let data_type = array.data_type().to_owned();
        cast_from_null_to_other(&data_type);

        // Cast null from and to struct
        let data_type =
            DataType::Struct(vec![Field::new("data", DataType::Int64, false)].into());
        cast_from_null_to_other(&data_type);
    }

    /// Print the `DictionaryArray` `array` as a vector of strings
    fn array_to_strings(array: &ArrayRef) -> Vec<String> {
        (0..array.len())
            .map(|i| {
                if array.is_null(i) {
                    "null".to_string()
                } else {
                    array_value_to_string(array, i).expect("Convert array to String")
                }
            })
            .collect()
    }

    #[test]
    fn test_cast_utf8_to_date32() {
        use chrono::NaiveDate;
        let from_ymd = chrono::NaiveDate::from_ymd_opt;
        let since = chrono::NaiveDate::signed_duration_since;

        let a = StringArray::from(vec![
            "2000-01-01",          // valid date with leading 0s
            "2000-2-2",            // valid date without leading 0s
            "2000-00-00",          // invalid month and day
            "2000-01-01T12:00:00", // date + time is invalid
            "2000",                // just a year is invalid
        ]);
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Date32).unwrap();
        let c = b.as_any().downcast_ref::<Date32Array>().unwrap();

        // test valid inputs
        let date_value = since(
            NaiveDate::from_ymd_opt(2000, 1, 1).unwrap(),
            from_ymd(1970, 1, 1).unwrap(),
        )
        .num_days() as i32;
        assert!(c.is_valid(0)); // "2000-01-01"
        assert_eq!(date_value, c.value(0));

        let date_value = since(
            NaiveDate::from_ymd_opt(2000, 2, 2).unwrap(),
            from_ymd(1970, 1, 1).unwrap(),
        )
        .num_days() as i32;
        assert!(c.is_valid(1)); // "2000-2-2"
        assert_eq!(date_value, c.value(1));

        // test invalid inputs
        assert!(!c.is_valid(2)); // "2000-00-00"
        assert!(!c.is_valid(3)); // "2000-01-01T12:00:00"
        assert!(!c.is_valid(4)); // "2000"
    }

    #[test]
    fn test_cast_utf8_to_date64() {
        let a = StringArray::from(vec![
            "2000-01-01T12:00:00", // date + time valid
            "2020-12-15T12:34:56", // date + time valid
            "2020-2-2T12:34:56",   // valid date time without leading 0s
            "2000-00-00T12:00:00", // invalid month and day
            "2000-01-01 12:00:00", // missing the 'T'
            "2000-01-01",          // just a date is invalid
        ]);
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Date64).unwrap();
        let c = b.as_any().downcast_ref::<Date64Array>().unwrap();

        // test valid inputs
        assert!(c.is_valid(0)); // "2000-01-01T12:00:00"
        assert_eq!(946728000000, c.value(0));
        assert!(c.is_valid(1)); // "2020-12-15T12:34:56"
        assert_eq!(1608035696000, c.value(1));
        assert!(c.is_valid(2)); // "2020-2-2T12:34:56"
        assert_eq!(1580646896000, c.value(2));

        // test invalid inputs
        assert!(!c.is_valid(3)); // "2000-00-00T12:00:00"
        assert!(!c.is_valid(4)); // "2000-01-01 12:00:00"
        assert!(!c.is_valid(5)); // "2000-01-01"
    }

    #[test]
    fn test_cast_list_containers() {
        // large-list to list
        let array = Arc::new(make_large_list_array()) as ArrayRef;
        let list_array = cast(
            &array,
            &DataType::List(Arc::new(Field::new("", DataType::Int32, false))),
        )
        .unwrap();
        let actual = list_array.as_any().downcast_ref::<ListArray>().unwrap();
        let expected = array.as_any().downcast_ref::<LargeListArray>().unwrap();

        assert_eq!(&expected.value(0), &actual.value(0));
        assert_eq!(&expected.value(1), &actual.value(1));
        assert_eq!(&expected.value(2), &actual.value(2));

        // list to large-list
        let array = Arc::new(make_list_array()) as ArrayRef;
        let large_list_array = cast(
            &array,
            &DataType::LargeList(Arc::new(Field::new("", DataType::Int32, false))),
        )
        .unwrap();
        let actual = large_list_array
            .as_any()
            .downcast_ref::<LargeListArray>()
            .unwrap();
        let expected = array.as_any().downcast_ref::<ListArray>().unwrap();

        assert_eq!(&expected.value(0), &actual.value(0));
        assert_eq!(&expected.value(1), &actual.value(1));
        assert_eq!(&expected.value(2), &actual.value(2));
    }

    fn make_list_array() -> ListArray {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7]))
            .build()
            .unwrap();

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1, 2], [3, 4, 5], [6, 7]]
        let value_offsets = Buffer::from_slice_ref([0, 3, 6, 8]);

        // Construct a list array from the above two
        let list_data_type =
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .build()
            .unwrap();
        ListArray::from(list_data)
    }

    fn make_large_list_array() -> LargeListArray {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7]))
            .build()
            .unwrap();

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1, 2], [3, 4, 5], [6, 7]]
        let value_offsets = Buffer::from_slice_ref([0i64, 3, 6, 8]);

        // Construct a list array from the above two
        let list_data_type =
            DataType::LargeList(Arc::new(Field::new("item", DataType::Int32, true)));
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .build()
            .unwrap();
        LargeListArray::from(list_data)
    }

    #[test]
    fn test_utf8_cast_offsets() {
        // test if offset of the array is taken into account during cast
        let str_array = StringArray::from(vec!["a", "b", "c"]);
        let str_array = str_array.slice(1, 2);

        let out = cast(&str_array, &DataType::LargeUtf8).unwrap();

        let large_str_array = out.as_any().downcast_ref::<LargeStringArray>().unwrap();
        let strs = large_str_array.into_iter().flatten().collect::<Vec<_>>();
        assert_eq!(strs, &["b", "c"])
    }

    #[test]
    fn test_list_cast_offsets() {
        // test if offset of the array is taken into account during cast
        let array1 = make_list_array().slice(1, 2);
        let array2 = Arc::new(make_list_array()) as ArrayRef;

        let dt = DataType::LargeList(Arc::new(Field::new("item", DataType::Int32, true)));
        let out1 = cast(&array1, &dt).unwrap();
        let out2 = cast(&array2, &dt).unwrap();

        assert_eq!(&out1, &out2.slice(1, 2))
    }

    #[test]
    fn test_list_to_string() {
        let str_array = StringArray::from(vec!["a", "b", "c", "d", "e", "f", "g", "h"]);
        let value_offsets = Buffer::from_slice_ref([0, 3, 6, 8]);
        let value_data = str_array.into_data();

        let list_data_type =
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .build()
            .unwrap();
        let array = Arc::new(ListArray::from(list_data)) as ArrayRef;

        let out = cast(&array, &DataType::Utf8).unwrap();
        let out = out
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(&out, &vec!["[a, b, c]", "[d, e, f]", "[g, h]"]);

        let out = cast(&array, &DataType::LargeUtf8).unwrap();
        let out = out
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap()
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(&out, &vec!["[a, b, c]", "[d, e, f]", "[g, h]"]);

        let array = Arc::new(make_list_array()) as ArrayRef;
        let out = cast(&array, &DataType::Utf8).unwrap();
        let out = out
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(&out, &vec!["[0, 1, 2]", "[3, 4, 5]", "[6, 7]"]);

        let array = Arc::new(make_large_list_array()) as ArrayRef;
        let out = cast(&array, &DataType::LargeUtf8).unwrap();
        let out = out
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap()
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(&out, &vec!["[0, 1, 2]", "[3, 4, 5]", "[6, 7]"]);
    }

    #[test]
    fn test_cast_f64_to_decimal128() {
        // to reproduce https://github.com/apache/arrow-rs/issues/2997

        let decimal_type = DataType::Decimal128(18, 2);
        let array = Float64Array::from(vec![
            Some(0.0699999999),
            Some(0.0659999999),
            Some(0.0650000000),
            Some(0.0649999999),
        ]);
        let array = Arc::new(array) as ArrayRef;
        generate_cast_test_case!(
            &array,
            Decimal128Array,
            &decimal_type,
            vec![
                Some(7_i128), // round up
                Some(7_i128), // round up
                Some(7_i128), // round up
                Some(6_i128), // round down
            ]
        );

        let decimal_type = DataType::Decimal128(18, 3);
        let array = Float64Array::from(vec![
            Some(0.0699999999),
            Some(0.0659999999),
            Some(0.0650000000),
            Some(0.0649999999),
        ]);
        let array = Arc::new(array) as ArrayRef;
        generate_cast_test_case!(
            &array,
            Decimal128Array,
            &decimal_type,
            vec![
                Some(70_i128), // round up
                Some(66_i128), // round up
                Some(65_i128), // round down
                Some(65_i128), // round up
            ]
        );
    }

    #[test]
    fn test_cast_numeric_to_decimal128_overflow() {
        let array = Int64Array::from(vec![i64::MAX]);
        let array = Arc::new(array) as ArrayRef;
        let casted_array = cast_with_options(
            &array,
            &DataType::Decimal128(38, 30),
            &CastOptions { safe: true },
        );
        assert!(casted_array.is_ok());
        assert!(casted_array.unwrap().is_null(0));

        let casted_array = cast_with_options(
            &array,
            &DataType::Decimal128(38, 30),
            &CastOptions { safe: false },
        );
        assert!(casted_array.is_err());
    }

    #[test]
    fn test_cast_numeric_to_decimal256_overflow() {
        let array = Int64Array::from(vec![i64::MAX]);
        let array = Arc::new(array) as ArrayRef;
        let casted_array = cast_with_options(
            &array,
            &DataType::Decimal256(76, 76),
            &CastOptions { safe: true },
        );
        assert!(casted_array.is_ok());
        assert!(casted_array.unwrap().is_null(0));

        let casted_array = cast_with_options(
            &array,
            &DataType::Decimal256(76, 76),
            &CastOptions { safe: false },
        );
        assert!(casted_array.is_err());
    }

    #[test]
    fn test_cast_floating_point_to_decimal128_overflow() {
        let array = Float64Array::from(vec![f64::MAX]);
        let array = Arc::new(array) as ArrayRef;
        let casted_array = cast_with_options(
            &array,
            &DataType::Decimal128(38, 30),
            &CastOptions { safe: true },
        );
        assert!(casted_array.is_ok());
        assert!(casted_array.unwrap().is_null(0));

        let casted_array = cast_with_options(
            &array,
            &DataType::Decimal128(38, 30),
            &CastOptions { safe: false },
        );
        let err = casted_array.unwrap_err().to_string();
        let expected_error = "Cast error: Cannot cast to Decimal128(38, 30)";
        assert!(
            err.contains(expected_error),
            "did not find expected error '{expected_error}' in actual error '{err}'"
        );
    }

    #[test]
    fn test_cast_floating_point_to_decimal256_overflow() {
        let array = Float64Array::from(vec![f64::MAX]);
        let array = Arc::new(array) as ArrayRef;
        let casted_array = cast_with_options(
            &array,
            &DataType::Decimal256(76, 50),
            &CastOptions { safe: true },
        );
        assert!(casted_array.is_ok());
        assert!(casted_array.unwrap().is_null(0));

        let casted_array = cast_with_options(
            &array,
            &DataType::Decimal256(76, 50),
            &CastOptions { safe: false },
        );
        let err = casted_array.unwrap_err().to_string();
        let expected_error = "Cast error: Cannot cast to Decimal256(76, 50)";
        assert!(
            err.contains(expected_error),
            "did not find expected error '{expected_error}' in actual error '{err}'"
        );
    }

    #[test]
    fn test_cast_decimal128_to_decimal128_negative_scale() {
        let input_type = DataType::Decimal128(20, 0);
        let output_type = DataType::Decimal128(20, -1);
        assert!(can_cast_types(&input_type, &output_type));
        let array = vec![Some(1123450), Some(2123455), Some(3123456), None];
        let input_decimal_array = create_decimal_array(array, 20, 0).unwrap();
        let array = Arc::new(input_decimal_array) as ArrayRef;
        generate_cast_test_case!(
            &array,
            Decimal128Array,
            &output_type,
            vec![
                Some(112345_i128),
                Some(212346_i128),
                Some(312346_i128),
                None
            ]
        );

        let casted_array = cast(&array, &output_type).unwrap();
        let decimal_arr = casted_array.as_primitive::<Decimal128Type>();

        assert_eq!("1123450", decimal_arr.value_as_string(0));
        assert_eq!("2123460", decimal_arr.value_as_string(1));
        assert_eq!("3123460", decimal_arr.value_as_string(2));
    }

    #[test]
    fn test_cast_numeric_to_decimal128_negative() {
        let decimal_type = DataType::Decimal128(38, -1);
        let array = Arc::new(Int32Array::from(vec![
            Some(1123456),
            Some(2123456),
            Some(3123456),
        ])) as ArrayRef;

        let casted_array = cast(&array, &decimal_type).unwrap();
        let decimal_arr = casted_array.as_primitive::<Decimal128Type>();

        assert_eq!("1123450", decimal_arr.value_as_string(0));
        assert_eq!("2123450", decimal_arr.value_as_string(1));
        assert_eq!("3123450", decimal_arr.value_as_string(2));

        let array = Arc::new(Float32Array::from(vec![
            Some(1123.456),
            Some(2123.456),
            Some(3123.456),
        ])) as ArrayRef;

        let casted_array = cast(&array, &decimal_type).unwrap();
        let decimal_arr = casted_array.as_primitive::<Decimal128Type>();

        assert_eq!("1120", decimal_arr.value_as_string(0));
        assert_eq!("2120", decimal_arr.value_as_string(1));
        assert_eq!("3120", decimal_arr.value_as_string(2));
    }

    #[test]
    fn test_cast_decimal128_to_decimal128_negative() {
        let input_type = DataType::Decimal128(10, -1);
        let output_type = DataType::Decimal128(10, -2);
        assert!(can_cast_types(&input_type, &output_type));
        let array = vec![Some(123)];
        let input_decimal_array = create_decimal_array(array, 10, -1).unwrap();
        let array = Arc::new(input_decimal_array) as ArrayRef;
        generate_cast_test_case!(
            &array,
            Decimal128Array,
            &output_type,
            vec![Some(12_i128),]
        );

        let casted_array = cast(&array, &output_type).unwrap();
        let decimal_arr = casted_array.as_primitive::<Decimal128Type>();

        assert_eq!("1200", decimal_arr.value_as_string(0));

        let array = vec![Some(125)];
        let input_decimal_array = create_decimal_array(array, 10, -1).unwrap();
        let array = Arc::new(input_decimal_array) as ArrayRef;
        generate_cast_test_case!(
            &array,
            Decimal128Array,
            &output_type,
            vec![Some(13_i128),]
        );

        let casted_array = cast(&array, &output_type).unwrap();
        let decimal_arr = casted_array.as_primitive::<Decimal128Type>();

        assert_eq!("1300", decimal_arr.value_as_string(0));
    }

    #[test]
    fn test_cast_decimal128_to_decimal256_negative() {
        let input_type = DataType::Decimal128(10, 3);
        let output_type = DataType::Decimal256(10, 5);
        assert!(can_cast_types(&input_type, &output_type));
        let array = vec![Some(i128::MAX), Some(i128::MIN)];
        let input_decimal_array = create_decimal_array(array, 10, 3).unwrap();
        let array = Arc::new(input_decimal_array) as ArrayRef;

        let hundred = i256::from_i128(100);
        generate_cast_test_case!(
            &array,
            Decimal256Array,
            &output_type,
            vec![
                Some(i256::from_i128(i128::MAX).mul_wrapping(hundred)),
                Some(i256::from_i128(i128::MIN).mul_wrapping(hundred))
            ]
        );
    }

    #[test]
    fn test_parse_string_to_decimal() {
        assert_eq!(
            Decimal128Type::format_decimal(
                parse_string_to_decimal_native::<Decimal128Type>("123.45", 2).unwrap(),
                38,
                2,
            ),
            "123.45"
        );
        assert_eq!(
            Decimal128Type::format_decimal(
                parse_string_to_decimal_native::<Decimal128Type>("12345", 2).unwrap(),
                38,
                2,
            ),
            "12345.00"
        );
        assert_eq!(
            Decimal128Type::format_decimal(
                parse_string_to_decimal_native::<Decimal128Type>("0.12345", 2).unwrap(),
                38,
                2,
            ),
            "0.12"
        );
        assert_eq!(
            Decimal128Type::format_decimal(
                parse_string_to_decimal_native::<Decimal128Type>(".12345", 2).unwrap(),
                38,
                2,
            ),
            "0.12"
        );
        assert_eq!(
            Decimal128Type::format_decimal(
                parse_string_to_decimal_native::<Decimal128Type>(".1265", 2).unwrap(),
                38,
                2,
            ),
            "0.13"
        );
        assert_eq!(
            Decimal128Type::format_decimal(
                parse_string_to_decimal_native::<Decimal128Type>(".1265", 2).unwrap(),
                38,
                2,
            ),
            "0.13"
        );

        assert_eq!(
            Decimal256Type::format_decimal(
                parse_string_to_decimal_native::<Decimal256Type>("123.45", 3).unwrap(),
                38,
                3,
            ),
            "123.450"
        );
        assert_eq!(
            Decimal256Type::format_decimal(
                parse_string_to_decimal_native::<Decimal256Type>("12345", 3).unwrap(),
                38,
                3,
            ),
            "12345.000"
        );
        assert_eq!(
            Decimal256Type::format_decimal(
                parse_string_to_decimal_native::<Decimal256Type>("0.12345", 3).unwrap(),
                38,
                3,
            ),
            "0.123"
        );
        assert_eq!(
            Decimal256Type::format_decimal(
                parse_string_to_decimal_native::<Decimal256Type>(".12345", 3).unwrap(),
                38,
                3,
            ),
            "0.123"
        );
        assert_eq!(
            Decimal256Type::format_decimal(
                parse_string_to_decimal_native::<Decimal256Type>(".1265", 3).unwrap(),
                38,
                3,
            ),
            "0.127"
        );
    }

    fn test_cast_string_to_decimal(array: ArrayRef) {
        // Decimal128
        let output_type = DataType::Decimal128(38, 2);
        assert!(can_cast_types(array.data_type(), &output_type));

        let casted_array = cast(&array, &output_type).unwrap();
        let decimal_arr = casted_array.as_primitive::<Decimal128Type>();

        assert_eq!("123.45", decimal_arr.value_as_string(0));
        assert_eq!("1.23", decimal_arr.value_as_string(1));
        assert_eq!("0.12", decimal_arr.value_as_string(2));
        assert_eq!("0.13", decimal_arr.value_as_string(3));
        assert_eq!("1.26", decimal_arr.value_as_string(4));
        assert_eq!("12345.00", decimal_arr.value_as_string(5));
        assert_eq!("12345.00", decimal_arr.value_as_string(6));
        assert_eq!("0.12", decimal_arr.value_as_string(7));
        assert_eq!("12.23", decimal_arr.value_as_string(8));
        assert!(decimal_arr.is_null(9));
        assert_eq!("0.00", decimal_arr.value_as_string(10));
        assert_eq!("0.00", decimal_arr.value_as_string(11));
        assert!(decimal_arr.is_null(12));

        // Decimal256
        let output_type = DataType::Decimal256(76, 3);
        assert!(can_cast_types(array.data_type(), &output_type));

        let casted_array = cast(&array, &output_type).unwrap();
        let decimal_arr = casted_array.as_primitive::<Decimal256Type>();

        assert_eq!("123.450", decimal_arr.value_as_string(0));
        assert_eq!("1.235", decimal_arr.value_as_string(1));
        assert_eq!("0.123", decimal_arr.value_as_string(2));
        assert_eq!("0.127", decimal_arr.value_as_string(3));
        assert_eq!("1.263", decimal_arr.value_as_string(4));
        assert_eq!("12345.000", decimal_arr.value_as_string(5));
        assert_eq!("12345.000", decimal_arr.value_as_string(6));
        assert_eq!("0.123", decimal_arr.value_as_string(7));
        assert_eq!("12.234", decimal_arr.value_as_string(8));
        assert!(decimal_arr.is_null(9));
        assert_eq!("0.000", decimal_arr.value_as_string(10));
        assert_eq!("0.000", decimal_arr.value_as_string(11));
        assert!(decimal_arr.is_null(12));
    }

    #[test]
    fn test_cast_utf8_to_decimal() {
        let str_array = StringArray::from(vec![
            Some("123.45"),
            Some("1.2345"),
            Some("0.12345"),
            Some("0.1267"),
            Some("1.263"),
            Some("12345.0"),
            Some("12345"),
            Some("000.123"),
            Some("12.234000"),
            None,
            Some(""),
            Some(" "),
            None,
        ]);
        let array = Arc::new(str_array) as ArrayRef;

        test_cast_string_to_decimal(array);
    }

    #[test]
    fn test_cast_large_utf8_to_decimal() {
        let str_array = LargeStringArray::from(vec![
            Some("123.45"),
            Some("1.2345"),
            Some("0.12345"),
            Some("0.1267"),
            Some("1.263"),
            Some("12345.0"),
            Some("12345"),
            Some("000.123"),
            Some("12.234000"),
            None,
            Some(""),
            Some(" "),
            None,
        ]);
        let array = Arc::new(str_array) as ArrayRef;

        test_cast_string_to_decimal(array);
    }

    #[test]
    fn test_cast_invalid_utf8_to_decimal() {
        let str_array = StringArray::from(vec!["4.4.5", ". 0.123"]);
        let array = Arc::new(str_array) as ArrayRef;

        // Safe cast
        let output_type = DataType::Decimal128(38, 2);
        let casted_array = cast(&array, &output_type).unwrap();
        assert!(casted_array.is_null(0));
        assert!(casted_array.is_null(1));

        let output_type = DataType::Decimal256(76, 2);
        let casted_array = cast(&array, &output_type).unwrap();
        assert!(casted_array.is_null(0));
        assert!(casted_array.is_null(1));

        // Non-safe cast
        let output_type = DataType::Decimal128(38, 2);
        let str_array = StringArray::from(vec!["4.4.5"]);
        let array = Arc::new(str_array) as ArrayRef;
        let option = CastOptions { safe: false };
        let casted_err = cast_with_options(&array, &output_type, &option).unwrap_err();
        assert!(casted_err
            .to_string()
            .contains("Cannot cast string '4.4.5' to value of Decimal128(38, 10) type"));

        let str_array = StringArray::from(vec![". 0.123"]);
        let array = Arc::new(str_array) as ArrayRef;
        let casted_err = cast_with_options(&array, &output_type, &option).unwrap_err();
        assert!(casted_err.to_string().contains(
            "Cannot cast string '. 0.123' to value of Decimal128(38, 10) type"
        ));
    }

    fn test_cast_string_to_decimal128_overflow(overflow_array: ArrayRef) {
        let output_type = DataType::Decimal128(38, 2);
        let casted_array = cast(&overflow_array, &output_type).unwrap();
        let decimal_arr = casted_array.as_primitive::<Decimal128Type>();

        assert!(decimal_arr.is_null(0));
        assert!(decimal_arr.is_null(1));
        assert!(decimal_arr.is_null(2));
        assert_eq!(
            "999999999999999999999999999999999999.99",
            decimal_arr.value_as_string(3)
        );
        assert_eq!(
            "100000000000000000000000000000000000.00",
            decimal_arr.value_as_string(4)
        );
    }

    #[test]
    fn test_cast_utf8_to_decimal128_overflow() {
        let overflow_str_array = StringArray::from(vec![
            i128::MAX.to_string(),
            i128::MIN.to_string(),
            "99999999999999999999999999999999999999".to_string(),
            "999999999999999999999999999999999999.99".to_string(),
            "99999999999999999999999999999999999.999".to_string(),
        ]);
        let overflow_array = Arc::new(overflow_str_array) as ArrayRef;

        test_cast_string_to_decimal128_overflow(overflow_array);
    }

    #[test]
    fn test_cast_large_utf8_to_decimal128_overflow() {
        let overflow_str_array = LargeStringArray::from(vec![
            i128::MAX.to_string(),
            i128::MIN.to_string(),
            "99999999999999999999999999999999999999".to_string(),
            "999999999999999999999999999999999999.99".to_string(),
            "99999999999999999999999999999999999.999".to_string(),
        ]);
        let overflow_array = Arc::new(overflow_str_array) as ArrayRef;

        test_cast_string_to_decimal128_overflow(overflow_array);
    }

    fn test_cast_string_to_decimal256_overflow(overflow_array: ArrayRef) {
        let output_type = DataType::Decimal256(76, 2);
        let casted_array = cast(&overflow_array, &output_type).unwrap();
        let decimal_arr = casted_array.as_primitive::<Decimal256Type>();

        assert_eq!(
            "170141183460469231731687303715884105727.00",
            decimal_arr.value_as_string(0)
        );
        assert_eq!(
            "-170141183460469231731687303715884105728.00",
            decimal_arr.value_as_string(1)
        );
        assert_eq!(
            "99999999999999999999999999999999999999.00",
            decimal_arr.value_as_string(2)
        );
        assert_eq!(
            "999999999999999999999999999999999999.99",
            decimal_arr.value_as_string(3)
        );
        assert_eq!(
            "100000000000000000000000000000000000.00",
            decimal_arr.value_as_string(4)
        );
        assert!(decimal_arr.is_null(5));
        assert!(decimal_arr.is_null(6));
    }

    #[test]
    fn test_cast_utf8_to_decimal256_overflow() {
        let overflow_str_array = StringArray::from(vec![
            i128::MAX.to_string(),
            i128::MIN.to_string(),
            "99999999999999999999999999999999999999".to_string(),
            "999999999999999999999999999999999999.99".to_string(),
            "99999999999999999999999999999999999.999".to_string(),
            i256::MAX.to_string(),
            i256::MIN.to_string(),
        ]);
        let overflow_array = Arc::new(overflow_str_array) as ArrayRef;

        test_cast_string_to_decimal256_overflow(overflow_array);
    }

    #[test]
    fn test_cast_large_utf8_to_decimal256_overflow() {
        let overflow_str_array = LargeStringArray::from(vec![
            i128::MAX.to_string(),
            i128::MIN.to_string(),
            "99999999999999999999999999999999999999".to_string(),
            "999999999999999999999999999999999999.99".to_string(),
            "99999999999999999999999999999999999.999".to_string(),
            i256::MAX.to_string(),
            i256::MIN.to_string(),
        ]);
        let overflow_array = Arc::new(overflow_str_array) as ArrayRef;

        test_cast_string_to_decimal256_overflow(overflow_array);
    }

    #[test]
    fn test_cast_date32_to_timestamp() {
        let a = Date32Array::from(vec![Some(18628), Some(18993), None]); // 2021-1-1, 2022-1-1
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Timestamp(TimeUnit::Second, None)).unwrap();
        let c = b.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
        assert_eq!(1609459200, c.value(0));
        assert_eq!(1640995200, c.value(1));
        assert!(c.is_null(2));
    }

    #[test]
    fn test_cast_date32_to_timestamp_ms() {
        let a = Date32Array::from(vec![Some(18628), Some(18993), None]); // 2021-1-1, 2022-1-1
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Timestamp(TimeUnit::Millisecond, None)).unwrap();
        let c = b
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(1609459200000, c.value(0));
        assert_eq!(1640995200000, c.value(1));
        assert!(c.is_null(2));
    }

    #[test]
    fn test_cast_date32_to_timestamp_us() {
        let a = Date32Array::from(vec![Some(18628), Some(18993), None]); // 2021-1-1, 2022-1-1
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();
        let c = b
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(1609459200000000, c.value(0));
        assert_eq!(1640995200000000, c.value(1));
        assert!(c.is_null(2));
    }

    #[test]
    fn test_cast_date32_to_timestamp_ns() {
        let a = Date32Array::from(vec![Some(18628), Some(18993), None]); // 2021-1-1, 2022-1-1
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Timestamp(TimeUnit::Nanosecond, None)).unwrap();
        let c = b
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(1609459200000000000, c.value(0));
        assert_eq!(1640995200000000000, c.value(1));
        assert!(c.is_null(2));
    }

    #[test]
    fn test_timezone_cast() {
        let a = StringArray::from(vec![
            "2000-01-01T12:00:00", // date + time valid
            "2020-12-15T12:34:56", // date + time valid
        ]);
        let array = Arc::new(a) as ArrayRef;
        let b = cast(&array, &DataType::Timestamp(TimeUnit::Nanosecond, None)).unwrap();
        let v = b.as_primitive::<TimestampNanosecondType>();

        assert_eq!(v.value(0), 946728000000000000);
        assert_eq!(v.value(1), 1608035696000000000);

        let b = cast(
            &b,
            &DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
        )
        .unwrap();
        let v = b.as_primitive::<TimestampNanosecondType>();

        assert_eq!(v.value(0), 946728000000000000);
        assert_eq!(v.value(1), 1608035696000000000);

        let b = cast(
            &b,
            &DataType::Timestamp(TimeUnit::Millisecond, Some("+02:00".into())),
        )
        .unwrap();
        let v = b.as_primitive::<TimestampMillisecondType>();

        assert_eq!(v.value(0), 946728000000);
        assert_eq!(v.value(1), 1608035696000);
    }

    #[test]
    fn test_cast_utf8_to_timestamp() {
        fn test_tz(tz: Arc<str>) {
            let valid = StringArray::from(vec![
                "2023-01-01 04:05:06.789000-08:00",
                "2023-01-01 04:05:06.789000-07:00",
                "2023-01-01 04:05:06.789 -0800",
                "2023-01-01 04:05:06.789 -08:00",
                "2023-01-01 040506 +0730",
                "2023-01-01 040506 +07:30",
                "2023-01-01 04:05:06.789",
                "2023-01-01 04:05:06",
                "2023-01-01",
            ]);

            let array = Arc::new(valid) as ArrayRef;
            let b = cast_with_options(
                &array,
                &DataType::Timestamp(TimeUnit::Nanosecond, Some(tz.clone())),
                &CastOptions { safe: false },
            )
            .unwrap();

            let tz = tz.as_ref().parse().unwrap();

            let as_tz = |v: i64| {
                as_datetime_with_timezone::<TimestampNanosecondType>(v, tz).unwrap()
            };

            let as_utc = |v: &i64| as_tz(*v).naive_utc().to_string();
            let as_local = |v: &i64| as_tz(*v).naive_local().to_string();

            let values = b.as_primitive::<TimestampNanosecondType>().values();
            let utc_results: Vec<_> = values.iter().map(as_utc).collect();
            let local_results: Vec<_> = values.iter().map(as_local).collect();

            // Absolute timestamps should be parsed preserving the same UTC instant
            assert_eq!(
                &utc_results[..6],
                &[
                    "2023-01-01 12:05:06.789".to_string(),
                    "2023-01-01 11:05:06.789".to_string(),
                    "2023-01-01 12:05:06.789".to_string(),
                    "2023-01-01 12:05:06.789".to_string(),
                    "2022-12-31 20:35:06".to_string(),
                    "2022-12-31 20:35:06".to_string(),
                ]
            );
            // Non-absolute timestamps should be parsed preserving the same local instant
            assert_eq!(
                &local_results[6..],
                &[
                    "2023-01-01 04:05:06.789".to_string(),
                    "2023-01-01 04:05:06".to_string(),
                    "2023-01-01 00:00:00".to_string()
                ]
            )
        }

        test_tz("+00:00".into());
        test_tz("+02:00".into());
    }

    #[test]
    fn test_cast_invalid_utf8() {
        let v1: &[u8] = b"\xFF invalid";
        let v2: &[u8] = b"\x00 Foo";
        let s = BinaryArray::from(vec![v1, v2]);
        let options = CastOptions { safe: true };
        let array = cast_with_options(&s, &DataType::Utf8, &options).unwrap();
        let a = array.as_string::<i32>();
        a.to_data().validate_full().unwrap();

        assert_eq!(a.null_count(), 1);
        assert_eq!(a.len(), 2);
        assert!(a.is_null(0));
        assert_eq!(a.value(0), "");
        assert_eq!(a.value(1), "\x00 Foo");
    }

    #[test]
    fn test_cast_utf8_to_timestamptz() {
        let valid = StringArray::from(vec!["2023-01-01"]);

        let array = Arc::new(valid) as ArrayRef;
        let b = cast(
            &array,
            &DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
        )
        .unwrap();

        let expect = DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into()));

        assert_eq!(b.data_type(), &expect);
        let c = b
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(1672531200000000000, c.value(0));
    }

    #[test]
    fn test_cast_decimal_to_utf8() {
        fn test_decimal_to_string<IN: ArrowPrimitiveType, OffsetSize: OffsetSizeTrait>(
            output_type: DataType,
            array: PrimitiveArray<IN>,
        ) {
            let b = cast(&array, &output_type).unwrap();

            assert_eq!(b.data_type(), &output_type);
            let c = b.as_string::<OffsetSize>();

            assert_eq!("1123.454", c.value(0));
            assert_eq!("2123.456", c.value(1));
            assert_eq!("-3123.453", c.value(2));
            assert_eq!("-3123.456", c.value(3));
            assert_eq!("0.000", c.value(4));
            assert_eq!("0.123", c.value(5));
            assert_eq!("1234.567", c.value(6));
            assert_eq!("-1234.567", c.value(7));
            assert!(c.is_null(8));
        }
        let array128: Vec<Option<i128>> = vec![
            Some(1123454),
            Some(2123456),
            Some(-3123453),
            Some(-3123456),
            Some(0),
            Some(123),
            Some(123456789),
            Some(-123456789),
            None,
        ];

        let array256: Vec<Option<i256>> =
            array128.iter().map(|v| v.map(i256::from_i128)).collect();

        test_decimal_to_string::<arrow_array::types::Decimal128Type, i32>(
            DataType::Utf8,
            create_decimal_array(array128.clone(), 7, 3).unwrap(),
        );
        test_decimal_to_string::<arrow_array::types::Decimal128Type, i64>(
            DataType::LargeUtf8,
            create_decimal_array(array128, 7, 3).unwrap(),
        );
        test_decimal_to_string::<arrow_array::types::Decimal256Type, i32>(
            DataType::Utf8,
            create_decimal256_array(array256.clone(), 7, 3).unwrap(),
        );
        test_decimal_to_string::<arrow_array::types::Decimal256Type, i64>(
            DataType::LargeUtf8,
            create_decimal256_array(array256, 7, 3).unwrap(),
        );
    }

    #[test]
    fn test_cast_numeric_to_decimal128_precision_overflow() {
        let array = Int64Array::from(vec![1234567]);
        let array = Arc::new(array) as ArrayRef;
        let casted_array = cast_with_options(
            &array,
            &DataType::Decimal128(7, 3),
            &CastOptions { safe: true },
        );
        assert!(casted_array.is_ok());
        assert!(casted_array.unwrap().is_null(0));

        let err = cast_with_options(
            &array,
            &DataType::Decimal128(7, 3),
            &CastOptions { safe: false },
        );
        assert_eq!("Invalid argument error: 1234567000 is too large to store in a Decimal128 of precision 7. Max is 9999999", err.unwrap_err().to_string());
    }

    #[test]
    fn test_cast_numeric_to_decimal256_precision_overflow() {
        let array = Int64Array::from(vec![1234567]);
        let array = Arc::new(array) as ArrayRef;
        let casted_array = cast_with_options(
            &array,
            &DataType::Decimal256(7, 3),
            &CastOptions { safe: true },
        );
        assert!(casted_array.is_ok());
        assert!(casted_array.unwrap().is_null(0));

        let err = cast_with_options(
            &array,
            &DataType::Decimal256(7, 3),
            &CastOptions { safe: false },
        );
        assert_eq!("Invalid argument error: 1234567000 is too large to store in a Decimal256 of precision 7. Max is 9999999", err.unwrap_err().to_string());
    }

    /// helper function to test casting from duration to interval
    fn cast_from_duration_to_interval<T: ArrowTemporalType>(
        array: Vec<i64>,
        cast_options: &CastOptions,
    ) -> Result<PrimitiveArray<IntervalMonthDayNanoType>, ArrowError>
    where
        arrow_array::PrimitiveArray<T>: From<Vec<i64>>,
    {
        let array = PrimitiveArray::<T>::from(array);
        let array = Arc::new(array) as ArrayRef;
        let casted_array = cast_with_options(
            &array,
            &DataType::Interval(IntervalUnit::MonthDayNano),
            cast_options,
        )?;
        casted_array
            .as_any()
            .downcast_ref::<IntervalMonthDayNanoArray>()
            .ok_or_else(|| {
                ArrowError::ComputeError(
                    "Failed to downcast to IntervalMonthDayNanoArray".to_string(),
                )
            })
            .cloned()
    }

    #[test]
    fn test_cast_from_duration_to_interval() {
        // from duration second to interval month day nano
        let array = vec![1234567];
        let casted_array = cast_from_duration_to_interval::<DurationSecondType>(
            array,
            &DEFAULT_CAST_OPTIONS,
        )
        .unwrap();
        assert_eq!(
            casted_array.data_type(),
            &DataType::Interval(IntervalUnit::MonthDayNano)
        );
        assert_eq!(casted_array.value(0), 1234567000000000);

        let array = vec![i64::MAX];
        let casted_array = cast_from_duration_to_interval::<DurationSecondType>(
            array.clone(),
            &DEFAULT_CAST_OPTIONS,
        )
        .unwrap();
        assert!(!casted_array.is_valid(0));

        let casted_array = cast_from_duration_to_interval::<DurationSecondType>(
            array,
            &CastOptions { safe: false },
        );
        assert!(casted_array.is_err());

        // from duration millisecond to interval month day nano
        let array = vec![1234567];
        let casted_array = cast_from_duration_to_interval::<DurationMillisecondType>(
            array,
            &DEFAULT_CAST_OPTIONS,
        )
        .unwrap();
        assert_eq!(
            casted_array.data_type(),
            &DataType::Interval(IntervalUnit::MonthDayNano)
        );
        assert_eq!(casted_array.value(0), 1234567000000);

        let array = vec![i64::MAX];
        let casted_array = cast_from_duration_to_interval::<DurationMillisecondType>(
            array.clone(),
            &DEFAULT_CAST_OPTIONS,
        )
        .unwrap();
        assert!(!casted_array.is_valid(0));

        let casted_array = cast_from_duration_to_interval::<DurationMillisecondType>(
            array,
            &CastOptions { safe: false },
        );
        assert!(casted_array.is_err());

        // from duration microsecond to interval month day nano
        let array = vec![1234567];
        let casted_array = cast_from_duration_to_interval::<DurationMicrosecondType>(
            array,
            &DEFAULT_CAST_OPTIONS,
        )
        .unwrap();
        assert_eq!(
            casted_array.data_type(),
            &DataType::Interval(IntervalUnit::MonthDayNano)
        );
        assert_eq!(casted_array.value(0), 1234567000);

        let array = vec![i64::MAX];
        let casted_array = cast_from_duration_to_interval::<DurationMicrosecondType>(
            array.clone(),
            &DEFAULT_CAST_OPTIONS,
        )
        .unwrap();
        assert!(!casted_array.is_valid(0));

        let casted_array = cast_from_duration_to_interval::<DurationMicrosecondType>(
            array,
            &CastOptions { safe: false },
        );
        assert!(casted_array.is_err());

        // from duration nanosecond to interval month day nano
        let array = vec![1234567];
        let casted_array = cast_from_duration_to_interval::<DurationNanosecondType>(
            array,
            &DEFAULT_CAST_OPTIONS,
        )
        .unwrap();
        assert_eq!(
            casted_array.data_type(),
            &DataType::Interval(IntervalUnit::MonthDayNano)
        );
        assert_eq!(casted_array.value(0), 1234567);

        let array = vec![i64::MAX];
        let casted_array = cast_from_duration_to_interval::<DurationNanosecondType>(
            array,
            &CastOptions { safe: false },
        )
        .unwrap();
        assert_eq!(casted_array.value(0), 9223372036854775807);
    }

    // helper function to test casting from interval to duration
    fn cast_from_interval_to_duration<T: ArrowTemporalType>(
        array: Vec<i128>,
        cast_options: &CastOptions,
    ) -> Result<PrimitiveArray<T>, ArrowError> {
        let array = IntervalMonthDayNanoArray::from(array);
        let array = Arc::new(array) as ArrayRef;
        let casted_array = cast_with_options(&array, &T::DATA_TYPE, cast_options)?;
        casted_array
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .ok_or_else(|| {
                ArrowError::ComputeError(format!(
                    "Failed to downcast to {}",
                    T::DATA_TYPE
                ))
            })
            .cloned()
    }

    #[test]
    fn test_cast_from_interval_to_duration() {
        // from interval month day nano to duration second
        let array = vec![1234567];
        let casted_array = cast_from_interval_to_duration::<DurationSecondType>(
            array,
            &DEFAULT_CAST_OPTIONS,
        )
        .unwrap();
        assert_eq!(
            casted_array.data_type(),
            &DataType::Duration(TimeUnit::Second)
        );
        assert_eq!(casted_array.value(0), 0);

        let array = vec![i128::MAX];
        let casted_array = cast_from_interval_to_duration::<DurationSecondType>(
            array.clone(),
            &DEFAULT_CAST_OPTIONS,
        )
        .unwrap();
        assert!(!casted_array.is_valid(0));

        let casted_array = cast_from_interval_to_duration::<DurationSecondType>(
            array,
            &CastOptions { safe: false },
        );
        assert!(casted_array.is_err());

        // from interval month day nano to duration millisecond
        let array = vec![1234567];
        let casted_array = cast_from_interval_to_duration::<DurationMillisecondType>(
            array,
            &DEFAULT_CAST_OPTIONS,
        )
        .unwrap();
        assert_eq!(casted_array.value(0), 1);

        let array = vec![i128::MAX];
        let casted_array = cast_from_interval_to_duration::<DurationMillisecondType>(
            array.clone(),
            &DEFAULT_CAST_OPTIONS,
        )
        .unwrap();
        assert!(!casted_array.is_valid(0));

        let casted_array = cast_from_interval_to_duration::<DurationMillisecondType>(
            array,
            &CastOptions { safe: false },
        );
        assert!(casted_array.is_err());

        // from interval month day nano to duration microsecond
        let array = vec![1234567];
        let casted_array = cast_from_interval_to_duration::<DurationMicrosecondType>(
            array,
            &DEFAULT_CAST_OPTIONS,
        )
        .unwrap();
        assert_eq!(
            casted_array.data_type(),
            &DataType::Duration(TimeUnit::Microsecond)
        );
        assert_eq!(casted_array.value(0), 1234);

        let array = vec![i128::MAX];
        let casted_array = cast_from_interval_to_duration::<DurationMicrosecondType>(
            array.clone(),
            &DEFAULT_CAST_OPTIONS,
        )
        .unwrap();
        assert!(!casted_array.is_valid(0));

        let casted_array = cast_from_interval_to_duration::<DurationMicrosecondType>(
            array,
            &CastOptions { safe: false },
        );
        assert!(casted_array.is_err());

        // from interval month day nano to duration nanosecond
        let array = vec![1234567];
        let casted_array = cast_from_interval_to_duration::<DurationNanosecondType>(
            array,
            &DEFAULT_CAST_OPTIONS,
        )
        .unwrap();
        assert_eq!(
            casted_array.data_type(),
            &DataType::Duration(TimeUnit::Nanosecond)
        );
        assert_eq!(casted_array.value(0), 1234567);

        let array = vec![i128::MAX];
        let casted_array = cast_from_interval_to_duration::<DurationNanosecondType>(
            array.clone(),
            &DEFAULT_CAST_OPTIONS,
        )
        .unwrap();
        assert_eq!(
            casted_array.data_type(),
            &DataType::Duration(TimeUnit::Nanosecond)
        );
        assert!(!casted_array.is_valid(0));

        let casted_array = cast_from_interval_to_duration::<DurationNanosecondType>(
            array,
            &CastOptions { safe: false },
        );
        assert!(casted_array.is_err());
    }
}
