# Advanced features of appenders in DuckDB

<a id="20260926-query"></a>
## Query-based appender
### User's prompt

Question about `duckdb_appender_create_query`. I take it that `duckdb_appender_create_ext` (or `duckdb_appender_create`) corresponds to plain `INSERT` in SQL? And if I want to use any conflict resolution, i.e. `INSERT INTO`, `MERGE INTO`, then I need to use `duckdb_appender_create_query`? There's no tutorial section on how to use this latter API call in the DuckDB docs. 

And then another question, more on how DuckDB works internally. It seems we have to supply the logical types of the columns even though it could be discoverable from the table to insert to. Maybe it's a little involved for DuckDB to figure that out from a complicated SQL statement, but then prepared statements don't require the user to set the parameters' column types. Do you know why `duckdb_appender_create_query` is designed that way? It's more than a little inconvenient to use from the API user's perspective.


### Gemini response intro

### Standard Appenders vs. `duckdb_appender_create_query`

Standard appenders (`duckdb_appender_create` and `duckdb_appender_create_ext`) bypass the SQL parser, binder, and query execution engine. They write row/chunk data directly into DuckDB's internal storage manager and vector formats. Because they bypass SQL execution entirely, they **only support basic append operations** (`INSERT INTO table VALUES (...)`).

If you need any of the following, you **must use `duckdb_appender_create_query`**:
* **Conflict resolution**: `INSERT INTO ... ON CONFLICT (...) DO UPDATE / DO NOTHING`
* **Upsert / Merging**: `MERGE INTO ...`
* **Pre-insertion transformations**: Calling SQL functions on incoming data before insertion (e.g., `ST_GeomFromWKB(col1)` or converting timestamp units)
* **Filtered/Conditional appends**: `INSERT INTO ... SELECT ... WHERE condition`

Under the hood, `duckdb_appender_create_query` creates an in-memory virtual relation (named `"appended_data"` by default). Rows pushed through the appender feed into this virtual relation, which is streamed through your SQL query pipeline.


### Why Logical Types Are Required: Architectural Rationale

The requirement to pass explicit `duckdb_logical_type` objects stems from how DuckDB's query planner handles **Type Inference Direction** and **Catalog Resolution**.

#### 1. Top-Down (Prepared Statements) vs. Bottom-Up (Query Appenders)

* **Prepared Statements (`INSERT INTO target VALUES ($1, $2)`):**
  * Parameter placeholders (`$1`, `$2`) are **leaf nodes/sinks** inside a known target statement.
  * During the *binding phase*, DuckDB looks up `target` in the catalog, determines column 1 is `BIGINT` and column 2 is `VARCHAR`, and **propagates types top-down** (from target table down to `$1` and `$2`).

* **Query Appenders (`duckdb_appender_create_query`):**
  * The appender input (`appended_data`) is a **source relation** at the root of a query plan DAG.
  * Type inference in SQL query execution flows **bottom-up** (from source relations through expression trees to final output operators).
  * DuckDB cannot infer source types from the target table because the SQL query inside `duckdb_appender_create_query` can perform arbitrary transformations, such as:
    ```sql
    INSERT INTO target_table
    SELECT 
        col1 + 10,                            -- Expression transformation
        ST_GeomFromWKB(col2),                 -- Function overload resolution
        CAST(col3 AS VARCHAR) 
    FROM appended_data                        -- Source relation
    WHERE col4 > 0;
    ```

#### 2. Function Overload Resolution and Expression Binding

To build an optimized query execution plan for the query string, DuckDB's catalog binder must resolve expression types *before* data arrives:
* In `col1 + 10`, should integer or floating-point addition instructions be emitted?
* In `ST_GeomFromWKB(col2)`, which function signature matches `col2`? (e.g., `BLOB` vs `VARCHAR`)
* In `ON CONFLICT (id) DO UPDATE SET val = excluded.val`, what are the data types of the `excluded` pseudo-table?

Without explicitly declaring the types of `appended_data`, the binder would fail during query compilation because `appended_data` does not exist in any catalog.

#### 3. Ephemeral Catalog Schema Registration

When you call `duckdb_appender_create_query`, DuckDB dynamically registers a transient table function / virtual table scan in the query context. 

Since this virtual relation has no physical catalog entry on disk or in memory, supplying the `duckdb_logical_type` array is the mechanism by which you provide its schema definition to the catalog binder.


### Summary Comparison

| Feature | Standard Appender | Prepared Statement | Query Appender (`create_query`) |
| :--- | :--- | :--- | :--- |
| **Execution Path** | Direct to Storage Manager | SQL Engine (Prepared Plan) | SQL Engine (Streaming Virtual Source) |
| **Supports UPSERT / MERGE** | No | Yes | Yes |
| **Type Source** | Existing Catalog Table | Inferred Top-Down from Target | Declared Explicitly via C API |
| **Performance** | Highest (raw vector writes) | Moderate (parses/executes per batch) | High (streams chunk batches through plan) |

<FollowUp label="Would you like a C API snippet demonstrating how to set up duckdb_appender_create_query with ON CONFLICT?" query="Show a C API code example using duckdb_appender_create_query to perform an INSERT ... ON CONFLICT DO UPDATE (upsert) operation, including setting column types and names."/>


<a id="20260926-type-binding"></a>
## Type binding for parameters
### User's prompt

Thank you. I have a further question which may help me understand a bit more about the DuckDB C API. In the example you gave above containing `INSERT INTO target_table ... FROM appended_data ...`, if this was re-formulated as a prepared statement, where the columns (`col1`, `col2`, `col3`) are replaced by parameters (`$1`, `$2`, `$3`), wouldn't the prepared statement have problems determining the exact type also? It would be ambiguous whether `$1 + 10` (from your example) would mean integer addition or floating-point addition, for example. 

Is the necessity of "dynamic" binding, i.e. that the type is only known after user sets the value of the parameter, part of what makes insertion using prepared statements slower? In other words, every time the prepared statement (for each iteration) is executed the internal types used in the query must be re-bound? If so, that's a subtlety I didn't realize earlier. 

Also, in the `duckdb_type` enumeration there are the members `DUCKDB_TYPE_INVALID` and `DUCKDB_TYPE_ANY`. From my testing (not formally written in the API docs), it seems that formal parameters with ambiguous types in prepared statements like `SELECT $1` would report `DUCKDB_TYPE_INVALID`. Then what is `DUCKDB_TYPE_ANY`? It's clearly distinct from `DUCKDB_TYPE_VARIANT` (which appears to be an actual per-value tagged type at the database storage level).


### Gemini response intro

### 1. Parameter Type Resolution in Prepared Statements

When you prepare a statement like `INSERT INTO target_table SELECT $1 + 10, ...`, DuckDB determines parameter types during the **`prepare` phase**, long before any execution or value binding happens.

#### How DuckDB Resolves Ambiguous Parameter Types
DuckDB uses a set of type-deduction rules during binder execution:

1. **Top-Down & Contextual Inference**: If a parameter is part of an expression like `$1 + 10`, DuckDB inspects the literal `10` (which defaults to `INTEGER` or `BIGINT`) and infers that `$1` should match the numeric type accepted by the `+` operator for that literal.
2. **Fallback / Defaulting**: If an expression is completely unconstrained (e.g., `SELECT $1`), DuckDB cannot infer a type from context. In such cases:
   * The parameter's logical type defaults to `VARCHAR` or remains `INVALID` depending on the context.
   * When you inspect the parameter type via `duckdb_param_type(prepared_statement, 1)`, it returns `DUCKDB_TYPE_INVALID` (or `UNKNOWN` internally) to indicate that the parameter type was not bound to a concrete physical layout during preparation.


### 2. Why Prepared Statements Are Slower Than Appenders

Your intuition about type re-binding is logical, but **DuckDB does *not* re-bind or re-plan query types on every execution iteration.**

When you call `duckdb_prepare`, DuckDB produces a fixed **Physical Query Plan**. Calling `duckdb_bind_*` and `duckdb_execute_prepared` reuses that identical physical plan without re-evaluating types or re-compiling expressions.

The performance gap between prepared statements and appenders comes down to three structural architectural factors:

#### Reason 1: Bypassing the Execution Engine Entirely
* **Prepared Statement (`INSERT INTO ... VALUES ($1, $2)`)**: Executes through the entire query engine pipeline for every invocation—setting up pipeline state, creating thread tasks, pushing execution vectors through expression trees, and cleaning up state.
* **Appender (`duckdb_appender_append_*`)**: Directly writes row data into DuckDB's in-memory column vectors (**`DataChunk`** objects). It completely bypasses expression evaluation, query pipelines, and task scheduling.

#### Reason 2: Row-at-a-Time C API Overhead vs. Vectorized Chunk Writes
DuckDB is a columnar, vectorized engine. It operates internally on vectors of 2,048 rows at a time:
* Executing a prepared statement in a C loop (`bind` $\rightarrow$ `execute` $\rightarrow$ `bind` $\rightarrow$ `execute`) processes **1 row per query execution pipeline**, incurring high per-query lock overhead, memory allocations, and transaction logging.
* An Appender stage-buffers incoming rows directly into vector memory blocks. When a `DataChunk` fills up (2,048 rows) or when you call `duckdb_appender_flush`, it commits the vectors to the target table in a single bulk operation.

#### Reason 3: WAL and Transaction Commit Granularity
* Each `duckdb_execute_prepared` call inside an implicit transaction incurs Write-Ahead Log (WAL) writing and transaction boundary overhead.
* Appenders batch all writes into a single continuous transaction log stream.


### 3. `DUCKDB_TYPE_INVALID` vs. `DUCKDB_TYPE_ANY` vs. `DUCKDB_TYPE_VARIANT`

These three enum values serve completely distinct roles across type inference, binder rules, and storage:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           duckdb_type Enum                              │
├──────────────────────────┬──────────────────────┬───────────────────────┤
│   DUCKDB_TYPE_INVALID    │   DUCKDB_TYPE_ANY    │  DUCKDB_TYPE_VARIANT  │
│  (Unassigned / Unbound)  │  (Binder Wildcard)   │   (Storage Type)      │
└──────────────────────────┴──────────────────────┴───────────────────────┘
```

#### `DUCKDB_TYPE_INVALID` (Unassigned / Error State)
* **What it is**: A fallback or sentinel value representing an uninitialized, invalid, or unconstrained logical type.
* **Where it appears**: When a prepared statement parameter (like `SELECT $1`) cannot have its type inferred during statement preparation, or when querying the type of an uninitialized handle.
* **Storage**: Cannot exist as a stored column type.

#### `DUCKDB_TYPE_ANY` (Polymorphic Binder Wildcard)
* **What it is**: An **internal catalog / binding pseudo-type**. It acts as a wildcard type parameter for SQL functions and table functions.
* **Where it appears**: Used when defining polymorphic functions in DuckDB (e.g., `COALESCE(ANY, ANY)`, `TYPEOF(ANY)`, or custom scalar/aggregate functions). It tells DuckDB's function resolver: *"This function argument accepts a vector of any valid physical type."*
* **Storage**: Cannot exist as a concrete column type in a table. It is purely an instruction to the catalog binder during function signature matching.

#### `DUCKDB_TYPE_VARIANT` (Semi-Structured Dynamic Storage Type)
* **What it is**: A **concrete physical storage type** designed to hold semi-structured data (similar to `VARIANT` in Snowflake or JSON-like dynamic values).
* **Where it appears**: Used for table columns that must store values of different types per row (e.g., row 1 stores an `INTEGER`, row 2 stores a `VARCHAR`, row 3 stores a `STRUCT`). Each value retains a runtime type tag attached directly to the stored data inside the column block.
* **Storage**: A fully valid storage type that can be created in a table schema (`CREATE TABLE t (v VARIANT);`).

<FollowUp label="Would you like an example of low-level vectorized insertion using duckdb_append_data_chunk?" query="Show a DuckDB C API example demonstrating low-level vectorized bulk insertion using duckdb_create_data_chunk and duckdb_append_data_chunk instead of row-by-row appender calls."/>


<a id="20260926-type-casting"></a>
## Implicit type casts in prepared statements
### User's prompt

So what if the type of the type-ambiguous parameter changes between subsequent executions of the same prepared statement? What does DuckDB do then? 

(I'm not actually writing C code directly in my current project. I'm designing bindings for DuckDB to a high-level language actually. The language can be pretty efficient at "direct" access, but as expected memory-safety concerns come into play, which requires careful design to make it ergonomic while retaining the speed. So for these reasons I want to know what's particularly fast in DuckDB that's worth exposing in my target language.)


### Gemini response intro

When a statement is prepared via `duckdb_prepare`, DuckDB resolves and fixes each parameter's target `LogicalType` during the preparation phase. 

If you call a different `duckdb_bind_*` function on a subsequent execution of that same prepared handle, **DuckDB does not re-bind or re-plan the query**. Instead, it attempts an **implicit auto-cast** from the newly bound type to the parameter's fixed target type.

### What Happens in Practice

1. **Compatible Types (Successful Cast):**
   If parameter `$1` was inferred as `BIGINT` at prepare time:
   * Execution 1: `duckdb_bind_int64(stmt, 1, 100)` $\rightarrow$ Exact match.
   * Execution 2: `duckdb_bind_int32(stmt, 1, 200)` $\rightarrow$ Implicitly auto-casts `int32` $\rightarrow$ `BIGINT`.
   * Execution 3: `duckdb_bind_varchar(stmt, 1, "300")` $\rightarrow$ Implicitly parses and auto-casts string `"300"` $\rightarrow$ `BIGINT`.

2. **Incompatible Types (Runtime Error):**
   * Execution 4: `duckdb_bind_varchar(stmt, 1, "hello")` $\rightarrow$ DuckDB attempts to cast `"hello"` to `BIGINT`. When `duckdb_execute_prepared` is called, it returns `DuckDBError` with a runtime conversion error (`Conversion Exception: Could not convert string 'hello' to INT64`).


### High-Performance DuckDB C API Patterns for Language Bindings

Because DuckDB is a columnar, vectorized engine, exposing the right API primitives in high-level bindings can result in performance differences of multiple orders of magnitude.

#### 1. Low-Level Vectorized Appender (`duckdb_append_data_chunk`)
* **Avoid**: Exposing only row-at-a-time appender methods (`duckdb_append_int32`, `duckdb_append_varchar`) in a tight FFI loop. Inter-language FFI boundary calls per cell quickly dominate execution time.
* **Expose**: `duckdb_create_data_chunk` and `duckdb_append_data_chunk`.
* **Why**: Your language can populate continuous memory buffers for up to 2,048 rows in host memory and hand the entire chunk to DuckDB in a **single FFI call**.

#### 2. Direct Column Vector Inspection (`duckdb_vector_get_data`)
* **Avoid**: Iterating over result sets cell-by-cell using `duckdb_value_int32` or string extraction routines.
* **Expose**: Vector slice access. For query results, fetch the `duckdb_data_chunk` from the result set, get the raw data pointer of each column vector via `duckdb_vector_get_data(vector)`, and wrap the pointer in a non-owning array/slice/span native to your target language.
* **Memory Safety**: The resulting slice/span must borrow its lifetime from the parent `DataChunk` or `QueryResult` object. Once `duckdb_destroy_result` or `duckdb_destroy_data_chunk` is called, backing pointers become dangling.

#### 3. Zero-Copy Apache Arrow Integration
* **Expose**: DuckDB's native C Arrow C Data Interface functions (`duckdb_query_arrow`, `duckdb_arrow_scan`, `duckdb_arrow_to_result`).
* **Why**: If your high-level language ecosystem already supports Apache Arrow (or standard columnar memory buffers), passing Arrow schemas and arrays directly across the C boundary avoids copying data entirely.

#### 4. Type Validation at the FFI Boundary
* Since parameter target types are fixed at prepare time, your binding's prepared statement wrapper can query `duckdb_param_logical_type(stmt, idx)` once immediately after `duckdb_prepare`.
* Cache these logical types in your wrapper object. This allows your high-level language to validate or convert parameter types before making foreign function calls, avoiding C API error state overhead on bad inputs.


| API Feature | Pattern | Performance | FFI Overhead | Memory Safety Model |
| :--- | :--- | :--- | :--- | :--- |
| **Row Appender** | `duckdb_append_*` in a loop | Moderate | High ($N \times M$ calls) | Managed by DuckDB state |
| **Chunk Appender** | `duckdb_append_data_chunk` | Extremely High | Low (1 call per 2,048 rows) | Buffer owned by host until append |
| **Cell Extraction** | `duckdb_value_*` per cell | Low | High ($N \times M$ calls) | Managed by DuckDB result |
| **Vector Extraction** | `duckdb_vector_get_data` | Peak (Zero Copy) | Low (1 call per chunk) | Lifetime tied to `DataChunk` handle |



<a id="20260926-chunked-write"></a>
## Example of appending data in chunks
### User's prompt

Wow, interesting. I've actually been working on the per-value appender so far. While FFI from my language is actually fairly fast, and for my own use case presently it's probably "good enough", I can definitely see data-chunk writes taking performance to the next level. Since my language is multi-threaded (in the same way as Java), "memory safety" on an object does inevitably require some kind of exclusive-lock logic (no matter how much one tries to optimize it) and I can see that being problematic if one were to insert thousands or millions of rows. 

But I don't even have the concept of data-chunk writes yet. I've got data-chunk reads though implemented --- that's just to consume results of (read-only) queries. And in my opinion vectorized reads were *easier* to consume in my language than going through the individual per-value API calls anyway, so choosing vectorized/chunk reads was "no brainer". But I haven't done or even designed the API in my language for chunked writes. Could you first give an example, in C, of how one would perform a chunked write using the standard appender? (It isn't so obvious in the standard DuckDB docs.)


### Gemini response intro

Performing chunked writes via the appender follows the exact same mental model as chunked reads: you allocate a `duckdb_data_chunk`, obtain raw pointers to each column's vector buffer, populate up to 2,048 rows at a time, and hand the entire chunk to the appender in a single call.

For language bindings, this reduces FFI boundary crossings and lock acquisition count by a factor of up to 2,048$\times$.


### Complete C API Vectorized Appender Example

Here is a complete, working example using `duckdb_create_data_chunk`, raw vector writing, and `duckdb_append_data_chunk`.

```c
#include <duckdb.h>
#include <stdio.h>
#include <stdlib.h>

int main(void) {
    duckdb_database db;
    duckdb_connection con;

    if (duckdb_open(NULL, &db) == DuckDBError || duckdb_connect(db, &con) == DuckDBError) {
        fprintf(stderr, "Failed to open/connect to database.\n");
        return 1;
    }

    // 1. Create target table
    duckdb_query(con, "CREATE TABLE users (id INTEGER, name VARCHAR, score DOUBLE);", NULL);

    // 2. Define schema logical types matching the table columns
    idx_t col_count = 3;
    duckdb_logical_type types[3];
    types[0] = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
    types[1] = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    types[2] = duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE);

    // 3. Create a reusable DataChunk
    duckdb_data_chunk chunk;
    if (duckdb_create_data_chunk(types, col_count, &chunk) == DuckDBError) {
        fprintf(stderr, "Failed to create DataChunk.\n");
        return 1;
    }

    // 4. Initialize the Appender
    duckdb_appender appender;
    if (duckdb_appender_create(con, NULL, "users", &appender) == DuckDBError) {
        fprintf(stderr, "Failed to create appender: %s\n", duckdb_appender_error(appender));
        return 1;
    }

    idx_t vector_capacity = duckdb_vector_size(); // Standard default is 2048
    idx_t total_rows_to_insert = 10000;
    idx_t rows_written = 0;

    // 5. Bulk append loop
    while (rows_written < total_rows_to_insert) {
        // Calculate current batch size (up to vector capacity)
        idx_t batch_size = total_rows_to_insert - rows_written;
        if (batch_size > vector_capacity) {
            batch_size = vector_capacity;
        }

        // Get column vectors from chunk
        duckdb_vector id_vec    = duckdb_data_chunk_get_vector(chunk, 0);
        duckdb_vector name_vec  = duckdb_data_chunk_get_vector(chunk, 1);
        duckdb_vector score_vec = duckdb_data_chunk_get_vector(chunk, 2);

        // Get direct memory data pointers for fixed-size types
        int32_t *id_data    = (int32_t *)duckdb_vector_get_data(id_vec);
        double  *score_data = (double *)duckdb_vector_get_data(score_vec);

        // Populate column vectors for this batch
        for (idx_t i = 0; i < batch_size; i++) {
            idx_t current_id = rows_written + i;

            // Direct pointer write for primitive types
            id_data[i]    = (int32_t)current_id;
            score_data[i] = current_id * 1.25;

            // Use helper API for variable-length strings
            char name_buf[32];
            snprintf(name_buf, sizeof(name_buf), "User_%llu", (unsigned long long)current_id);
            duckdb_vector_assign_string_element(name_vec, i, name_buf);
        }

        // Inform the chunk how many valid rows were written
        duckdb_data_chunk_set_size(chunk, batch_size);

        // Push the entire chunk into the appender
        if (duckdb_append_data_chunk(appender, chunk) == DuckDBError) {
            fprintf(stderr, "Error appending chunk: %s\n", duckdb_appender_error(appender));
            break;
        }

        rows_written += batch_size;

        // Reset chunk state (clears vector size and internal string allocations) for reuse
        duckdb_data_chunk_reset(chunk);
    }

    // 6. Flush pending writes to storage engine
    duckdb_appender_flush(appender);
    duckdb_appender_destroy(&appender);

    // 7. Cleanup memory handles
    duckdb_destroy_data_chunk(&chunk);
    for (idx_t i = 0; i < col_count; i++) {
        duckdb_destroy_logical_type(&types[i]);
    }

    duckdb_disconnect(&con);
    duckdb_close(&db);

    printf("Successfully inserted %llu rows in chunks.\n", (unsigned long long)rows_written);
    return 0;
}
```


### Critical Considerations for Language Binding Architecture

1. **Direct Pointer Transfer (`memcpy`)**
   For primitive numeric types (`INT32`, `INT64`, `FLOAT`, `DOUBLE`, `TIMESTAMP`), your high-level language can copy entire arrays directly into `duckdb_vector_get_data()` using a single `memcpy` or block memory write, completely skipping per-element iteration loops in C.

2. **`duckdb_data_chunk_reset` Reuse**
   Creating a `duckdb_data_chunk` allocates internal vector buffers. Instead of creating and destroying handles inside your loop, allocate **one** chunk per thread, fill it, push it, call `duckdb_data_chunk_reset(chunk)`, and reuse it for the next batch.

3. **String Allocation (`duckdb_vector_assign_string_element`)**
   DuckDB stores short strings ($\le 12$ bytes) inline inside string vector slots and heap-allocates longer strings. Always use `duckdb_vector_assign_string_element` or `duckdb_vector_assign_string_element_len` rather than writing raw `duckdb_string_t` structs manually, as this ensures DuckDB manages heap string lifetimes safely.

4. **Handling `NULL` Values**
   DuckDB uses validity bitmasks for `NULL` handling:
   ```c
   uint64_t *validity = duckdb_vector_get_validity(vec);
   // To set row i as NULL:
   duckdb_validity_set_row_invalid(validity, i);
   ```
   If a column batch contains no `NULL` values, you do not need to touch the validity mask at all (it defaults to all valid).

