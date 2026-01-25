# TiFlash Computation Engine Guide

This directory contains the computation layer (Flash) of TiFlash, responsible for query planning, execution, and MPP (Massively Parallel Processing).

## 📂 Key Components

- **`Planner/`**: Transforms TiDB DAG requests into TiFlash physical plans.
  - `Planner`: Entry point for query planning.
  - `PhysicalPlanNode`: Represents physical operators in the plan tree.
- **`Coprocessor/`**: Handles TiDB Coprocessor and DAG requests.
  - `DAGContext`: Stores query-level state and metadata.
  - `DAGExpressionAnalyzer`: Analyzes and builds expressions from TiDB protobuf definitions.
- **`Mpp/`**: Implements the MPP framework for distributed query execution.
  - `MPPTask`: Represents a unit of execution on a single node.
  - `ExchangeReceiver` & `ExchangeSender`: Handle data distribution between MPP tasks.
- **`Pipeline/`**: The modern pipeline-based execution engine.
  - `Pipeline`: A sequence of operators that can be executed in parallel.
  - `Exec/`: Implementation of pipeline operators.
- **`Executor/`**: Bridges the gap between plans and execution (both DataStream and Pipeline).

## 🧪 Testing

Computation engine tests often use `gtests_dbms` and mock environments.

### Running Flash Tests
- **Planner Tests**:
  ```bash
  cmake-build-debug/dbms/gtests_dbms --gtest_filter="PlannerTest*"
  ```
- **MPP Tests**:
  ```bash
  cmake-build-debug/dbms/gtests_dbms --gtest_filter="MPP*"
  ```
- **Pipeline Tests**:
  ```bash
  cmake-build-debug/dbms/gtests_dbms --gtest_filter="Pipeline*"
  ```
- **Coprocessor Tests**:
  ```bash
  cmake-build-debug/dbms/gtests_dbms --gtest_filter="DAG*"
  ```

### Integration Tests
Many computation features are verified via integration tests in `tests/fullstack-test`.

## 🛠 Coding Patterns

- **Protobuf Interaction**: Flash code heavily interacts with TiDB's tipb (TiDB Protocol Buffers). Use `DAGRequest` and `tipb::Executor` classes.
- **Expression Handling**: Use `DAGExpressionAnalyzer` to handle TiDB functions and expressions.
- **Memory Management**: MPP and Pipeline engines use `MemoryTracker` to monitor and limit query memory usage.
- **Streaming**: Data is often processed in `Block` units (from ClickHouse). Understand `IBlockInputStream` and the new `Operator` interface in Pipeline.

## 💡 Architecture Note
TiFlash is transitioning from a `BlockInputStream`-based execution model to a `Pipeline`-based one. When adding new operators or features, prefer the `Pipeline` infrastructure if possible.
