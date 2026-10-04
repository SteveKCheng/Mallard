# Author's notes (Oct 4, 2026) on designing proper column/vector info/state structures for the chunk writer

## *Column/type info* versus *mutable vector state*

Two key points:

  - The type info is fixed!
  - BUT the vector resets when the chunk resets!

Should the type info be separated from the vector info?  Verdict: YES.

In fact if we want to support parallel creation of chunks with the same schema 
(analogous to chunks being able to be read in parallel), then the type info must be
immutable & thread-safe.

Another point about type info: `VectorElementConverter`, `DuckDbComplexTypeInfo` are already
recursive by their nature.  So only the *vector state* needs a new nested array structure!

Thus, `DuckDbChunkWriter` needs two parallel arrays:

  - One array for column/type info; flat structure
  - Another array for mutable vector state; recursive

## Logical type info

We need a standardized way for internals to "share" a native logical type instance
to avoid defensive copying all over the place.

One object, whether it be the chunk, or instance of `DuckDbComplexTypeInfo`, is designated
as the owner, and others borrow it through a standardized protocol: like
DuckDbStructColumns.BorrowNativeLogicalType.  For `DuckDbComplexTypeInfo`, we could achieve
this by moving the ref count and the native logical type pointer to be part of the base class,
and then "BorrowNativeLogicalType" would just be a base-class method.

In general, the structure for "fixed type info" looks a lot like DuckDbResult.Column.
Only the part that requires looking back at the DuckDB chunk/result needs to be re-implemented
differently for a writer than the reader:

 - `DuckDbResult.Column.Name` (`string`): 
   - taking a shared lock on DuckDB result object
   - calling duckdb_column_name
   - atomically caching (`cmpxchg`) the name in a <c>string?</c> field
 - `DuckDbResult.Column.Converter` (`VectorElementConverter`):
   - slightly complicated logic to handle boxing options
   - with possibly one level of recursive call
   - eventually needs to call CreateConverter
     - which takes lock
     - ... to call `ConverterCreationContext.Indexed.FromNativeResult`
     - ... all because the native logical type is not accessible otherwise 
     - If the native logical type is available in the first place this dance will be made easy
   - uses an `Antitear` field to cache the result

Most of the complicated logic that could be profitably re-used in `DuckDbResult.Column` is just
about caching `VectorElementConverter`s.  That code and its associated data (the cache) could
be re-factored into yet another internal struct type that `DuckDbResult.Column` could include
as a member.  The re-factored code will need to take some kind of delegate or interface
to obtain the necessary `duckdb_logical_type` (for a given column index).

Note that the *names* of the columns are not available from `duckdb_data_chunk`'s API but
only from the `duckdb_result`'s API, so that caching/retrieval logic for names *cannot* be
re-used for the writer.  In fact, it can't even be re-used for readers in different contexts
such as user-defined functions.

## The proper naming of classes and structures 

Now the author realizes that `DuckDbColumnInfo` should probably be named `DuckDbSimpleTypeInfo`
since:

  - the information there is about the *type* representation and not the column
  - it would contrast better with `DuckDbComplexTypeInfo` which contains more detailed information


