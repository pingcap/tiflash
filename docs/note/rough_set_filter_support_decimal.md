# Rough Set MinMax Filter Support Decimal

## Background

TiFlash rough set min/max filter currently supports several scalar types, but decimal support needs special handling.

TiFlash has four decimal physical types:

- `Decimal32`
- `Decimal64`
- `Decimal128`
- `Decimal256`

The min/max values stored in `MinMaxIndex` for decimal columns are raw decimal integer values inside `ColumnDecimal<T>`. These raw values do not carry scale information. The scale belongs to the column type, `DataTypeDecimal<T>`.

SQL constants are represented as `Field`. Decimal constants are stored as `DecimalField<T>`, and `DecimalField<T>` carries its own scale.

Because of this, rough set min/max filtering must compare logical decimal values instead of directly comparing raw integer storage values.

For example:

```text
1.23    scale=2 raw=123
1.23000 scale=5 raw=123000
```

These two values are logically equal, although their raw stored values are different.

## Goal

Support rough set min/max filtering for decimal columns when the pushed down predicate constant is also decimal.

Target predicates include:

```sql
decimal_col = 1.23
decimal_col > 1.23
decimal_col >= 1.23
decimal_col < 1.23
decimal_col <= 1.23
decimal_col IN (1.23, 4.56)
decimal_col <=> 1.23
```

The implementation should support decimal comparisons across physical decimal types and scales, for example:

- `Decimal64` column vs `Decimal64` literal with the same scale
- `Decimal64` column vs `Decimal32` literal with a different scale
- `Decimal128` column vs `Decimal64` literal
- `Decimal256` column vs `Decimal128` literal

## Non-Goal

This change should only support decimal-vs-decimal comparison.

Do not support decimal min/max comparison with non-decimal constants in this change.

Examples intentionally not optimized by rough set:

```sql
decimal_col = 1
decimal_col = 1.23e0
decimal_col = '1.23'
```

The assumption is that TiDB should normally cast constants to decimal before pushing predicates to TiFlash when the target column is decimal.

If TiFlash receives a non-decimal constant for a decimal column, rough set should return `ValueCompareResult::CanNotCompare`. The upper layer should then conservatively treat the pack as `RSResult::Some`.

This preserves correctness. The worst case is losing a rough set filtering opportunity.

## Existing Flow

`MinMaxIndex` stores per-pack min/max values in `minmaxes`.

The rough set check entry points include:

- `MinMaxIndex::checkCmp`
- `MinMaxIndex::checkIn`
- `MinMaxIndex::checkNullEqual`

These methods dispatch by column type and then call `RoughCheck`.

`RoughCheck` delegates the actual constant-vs-min/max comparison to:

```cpp
ValueComparision<Op>::compare(left_field, type, right_value)
```

Here:

- `left_field` is the predicate constant represented as `Field`.
- `type` is the column type.
- `right_value` is the raw min or max value read from `MinMaxIndex`.

## Design

### Decimal Column Dispatch

Add decimal dispatch in `MinMaxIndex` check paths:

```cpp
FOR_DECIMAL_TYPES(DISPATCH_DECIMAL)
```

The dispatch should cover nullable and non-nullable paths:

- `checkIn`
- `checkNullableIn`
- `checkCmp`
- `checkNullableCmp`
- `checkNullableNullEqual`

`checkNullEqual` can keep delegating to `checkCmp<RoughCheck::CheckEqual>` for non-nullable columns and `checkNullableNullEqual` for nullable columns.

### Decimal Index Generation

`DMFileWriter` must also create ordinary min/max index streams for decimal columns.

The write-side index predicate should include decimal explicitly:

```cpp
bool do_index = cd.id == EXTRA_HANDLE_COLUMN_ID || type->isInteger() || type->isDateOrDateTime()
    || type->isDecimal();
```

Do not expand this to all `isValueRepresentedByNumber()` types in this change. The intended scope is decimal only.

This change is required for real DMFiles. Without it, decimal comparison support in `MinMaxIndex` only works for manually constructed indexes in unit tests, but persisted DMFiles will not have decimal ordinary min/max payloads to load during rough set filtering.

### Decimal Min/Max Data Access

For normal numeric types, the existing code reads min/max data from `ColumnVector<T>`.

For decimal types, min/max data must be read from `ColumnDecimal<T>`:

```cpp
assert_cast<const ColumnDecimal<T> &>(column).getData()
```

A small helper can avoid duplicating the access logic:

```cpp
template <typename T>
const auto & minMaxColumnData(const IColumn & column)
{
    if constexpr (IsDecimal<T>)
        return assert_cast<const ColumnDecimal<T> &>(column).getData();
    else
        return toColumnVectorData<T>(column);
}
```

### Decimal Value Comparison

Extend `ValueComparision` to recognize decimal types on both sides.

Supported literal-side types:

- `DecimalField<Decimal32>`
- `DecimalField<Decimal64>`
- `DecimalField<Decimal128>`
- `DecimalField<Decimal256>`

Supported min/max-side types:

- `Decimal32`
- `Decimal64`
- `Decimal128`
- `Decimal256`

The supported decimal comparison pattern is:

```text
Field(DecimalField<L>) vs Decimal<R>
```

The literal side should be extracted as:

```cpp
const auto & left = left_field.safeGet<DecimalField<L>>();
auto left_value = left.getValue();
auto left_scale = left.getScale();
```

The min/max side should be handled as:

```cpp
auto right_value = right;
auto right_scale = getDecimalScale(*right_type, 0);
```

The actual comparison should use the existing execution-layer decimal comparison utility:

```cpp
DecimalComparison<LeftRaw, RightRaw, Op, true>::compare(
    left_value,
    right_value,
    left_scale,
    right_scale);
```

This avoids reimplementing scale alignment, cross-type comparison, and overflow handling.

### Unsupported Comparisons

If the comparison is not decimal-vs-decimal, `ValueComparision` should return `CanNotCompare` instead of throwing.

For rough set, unsupported comparison means the pack cannot be filtered by min/max and should be treated as `RSResult::Some`.

This is required for correctness. A failed rough set optimization must not reject packs.

## Correctness Notes

Decimal raw values cannot be compared directly when scales differ.

Literal scale and column scale must be provided separately:

- Literal scale comes from `DecimalField<T>::getScale()`.
- Column min/max scale comes from `DataTypeDecimal<T>::getScale()` through `getDecimalScale(*type, 0)`.

Nullable decimal should follow existing nullable rough set semantics:

- Packs with both NULL and non-NULL values should keep the current `SomeNull` style behavior.
- All-NULL packs should not match a non-NULL decimal constant.
- For `<=> decimal_literal`, if all non-NULL values match but the pack also contains NULL rows, the result should not be upgraded to `All`, because NULL rows do not match a non-NULL `<=>` literal.

## Test Plan

Update existing min/max rough set tests so `Decimal64` is no longer treated as unsupported.

Add targeted decimal tests covering a nullable decimal column such as:

```text
Nullable(Decimal64(20,5))
```

Suggested pack layout:

```text
pack0: 1.23000, 2.34000, NULL
pack1: 5.00000, 5.00000
pack2: NULL, NULL
```

Suggested checks:

- `col = 1.23`
- `col IN (1.23, 7.77)`
- `col > 2.34`
- `col >= 5.00`
- `col < 5.00`
- `col <= 1.23`
- `col <=> 5.00`

At minimum, include one cross-subtype and cross-scale comparison:

```text
Decimal64 column with scale 5 vs Decimal32 literal with scale 2
```

Also add a DMFile write-side regression test to verify `DMFileWriter` creates and persists the ordinary min/max `.idx`
payload for decimal columns. This guards against a read-side-only implementation where decimal rough set logic exists
but real DMFiles never contain decimal min/max indexes.

If practical, add direct `ValueComparision` tests for:

- `Decimal32` literal vs `Decimal64` min/max
- `Decimal64` literal vs `Decimal128` min/max
- `Decimal128` literal vs `Decimal256` min/max

## Validation

Run formatting for modified C++ files.

Run targeted gtest for min/max index:

```bash
gtests_dbms --gtest_filter='MinMaxIndexTest.*'
```

If full min/max index gtest is too expensive, run the focused decimal and comparison cases:

```bash
gtests_dbms --gtest_filter='MinMaxIndexTest.CheckDecimal:MinMaxIndexTest.CheckIn:MinMaxIndexTest.CheckCmpEqual:MinMaxIndexTest.CheckCmpGreater:MinMaxIndexTest.CheckCmpGreaterEqual:MinMaxIndexTest.CheckNullEqual'
```

Do not run Bazel for this change unless explicitly requested.
