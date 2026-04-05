# TiFlash Test Utilities Guide

This directory provides essential utilities and base classes for writing C++ unit tests in TiFlash.

## 📂 Core Utilities

- **`TiFlashTestBasic.h`**: Base class for general unit tests. Provides common assertions like `ASSERT_DATATYPE_EQ`, `ASSERT_FIELD_EQ`, and `ASSERT_STRVIEW_EQ`.
- **`ExecutorTestUtils.h`**: Base class (`ExecutorTest`) for testing query executors. Supports both `DataStream` and `Pipeline` execution models via `WRAP_FOR_TEST_BEGIN/END`.
- **`FunctionTestUtils.h`**: Utilities for testing individual functions and expressions.
- **`ColumnGenerator.h`**: Helper to generate random or structured data for various column types.
- **`FailPointUtils.h`**: Utilities to enable and trigger failpoints within tests.
- **`mockExecutor.h`**: Allows building mock DAG requests (TableScan, Join, Aggregation, etc.) for testing the computation layer without a full TiDB cluster.

## 🧪 Common Testing Patterns

### 1. Simple Unit Test
Inherit from `::testing::Test` or use `TiFlashTestEnv` to setup the environment.
```cpp
#include <TestUtils/TiFlashTestBasic.h>

class MyTest : public ::testing::Test { ... };
TEST_F(MyTest, Basic) {
    ASSERT_EQ(1, 1);
}
```

### 2. Function/Expression Test
Use `FunctionTestUtils` to verify the output of C++ functions.
```cpp
#include <TestUtils/FunctionTestUtils.h>

// Example: Testing a scalar function
ASSERT_COLUMN_EQ(
    createColumn<Int32>({2, 3}),
    executeFunction("plus", createColumn<Int32>({1, 2}), createColumn<Int32>({1, 1}))
);
```

### 3. Executor Test
Inherit from `ExecutorTest` and use `MockDAGRequestBuilder` to build and run queries.
```cpp
#include <TestUtils/ExecutorTestUtils.h>

class MyExecutorTest : public ExecutorTest { ... };
TEST_F(MyExecutorTest, Join) {
    auto request = context.scan("db", "table").join(...).build();
    executeAndAssertColumnsEqual(request, expected_columns);
}
```

### 4. Failpoint Test
Simulate error paths by triggering failpoints.
```cpp
#include <TestUtils/FailPointUtils.h>

TEST_F(MyTest, ErrorPath) {
    FailPointHelper::enableFailPoint("failpoint_name");
    // Run code that triggers failpoint
    FailPointHelper::disableFailPoint("failpoint_name");
}
```

## 🛠 Best Practices
- **Use `createColumn`**: Prefer the `createColumn<T>` helpers in `FunctionTestUtils.h` to easily build test data.
- **Test Pipeline & Non-Pipeline**: Wrap executor tests in `WRAP_FOR_TEST_BEGIN` / `WRAP_FOR_TEST_END` to ensure coverage for both execution engines.
- **Clean Up**: Ensure `TearDown()` or `FailPointHelper::disableFailPoint` is called to avoid side effects on other tests.
- **Check Data Types**: Use `ASSERT_DATATYPE_EQ` to verify that column types match expectations.
