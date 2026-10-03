# Coding Style and Design Standards

This file lists the coding rules and design standards chosen for Mallard, with rationale.

These lists are not exhaustive.  Rules that are implicitly followed in the codebase not explicitly mentioned yet will be added as development continues.

## Documentation

  - All methods and properties that is part of the public API surface should be documented
    - Anywhere this rule is being violated in this codebase should be considered as a bug and not to be imitated; the C# compiler is set to warn on those already
  - Write reference documentation with the standard XML syntax within the C# code.
  - Documentation may be omitted for: unit tests, private and internal members
    - But consider writing it anyway: helps understanding without having to read the body every time
    - "Obvious" functionality that is only a few lines long probably do not need additional comment
    - Subtleties such as ownership patterns/constraints of DuckDB native objects, or requirements on locking are worth documenting
    - Even though tracing through such subtleties is not too difficult --- because this codebase is relatively small --- documentation helps in discovering inconsistencies and bugs
  - Describe basic usage briefly under "remarks"
    - It is okay to duplicate a little from the official DuckDB documentation
    - Although the DuckDB documentation is definitely more official, a .NET user might not necessarily know how the C programming language or how the DuckDB C API translates to how DuckDB or Mallard works
  - Any non-trivial considerations about performance, correctness and design choices should be noted
    - Especially if they are not obvious to a casual user perusing the official DuckDB documentation
    - And there may be special considerations for Mallard
    - Users should not be expected to peruse Mallard's source code to discover such points
  - Document any exceptions thrown to a reasonable degree
    - Using the formal `<exception>` tag in the XML documentation is preferred
    - But when that is not practical, a list, not necessarily exhaustive, may be given in accompanying remarks
    - Ask the question: "what would a piece of robust software want to know in order to properly handle all operational errors?"
  - Tone for user documentation: assume the audience is an intelligent programmer
    - Write in English that's generally agreed to be good: do not overuse passive voice, nominalization, obscure abbreviations
    - Avoid corporate buzzwords & pep talk
    - It is okay to be opinionated when justifications for a design choice is adequately given

## Identifier names

  - Public members follow standard .NET naming conventions as established by Microsoft
    - Even if they conflict with DuckDB's native naming conventions
    - For example: our prefix for C# type identifiers is `DuckDb` (Microsoft capitalization rule) not `DuckDB`
  - Internal types and functions used only in interop with DuckDB should copy the DuckDB names for ease of reference
  - Most of Mallard's types at the .NET-namespace level should be prefixed with `DuckDb`
    - We expect most people would write code with the whole `Mallard` namespace imported (`using Mallard;`) and so unprefixed names would be hard to distinguish against types from other libraries if we do not take a unique prefix
  - Occasionally we prefer our own terminology over what DuckDB C API uses
    - When doing so would make the API easier to understand for .NET users
    - Or to disambiguate
    - For example: "type" is a much-overloaded word.  It could mean a .NET type (i.e. relating to `System.Type`) or a DuckDB (SQL) type.  That's already enough ambiguity, without adding yet another shade of meaning, as seen in `duckdb_error_type` from DuckDB's C API, which is actually an enumeration.  We will refer to such enumerations as "kinds"; the LLVM Project employs a similar convention. 
  - We do not prioritize naming consistency with DuckDB.NET
    - We consider recognizability of our identifiers with core DuckDB concepts to be good enough
    - Users who want code to be "portable" with DuckDB.NET can only use the standard ADO.NET interfaces and classes anyway
  - "Mallard" is only used in the names of .NET namespaces and NuGet packages
    - Rationale: "DuckDB" is what clients care about, not the branding of the API-binding layer
    - Naming clash with DuckDB.NET is not a concern since almost no client code would ever mix two binding libraries
  - Use adequately descriptive names even for private/internal identifiers 
    - … unless they have to follow the DuckDB C API as dictated by the other rules
  - Names of formal arguments to functions are not significant at the API/ABI level for C/C++
    - Therefore the DuckDB's naming may be somewhat sloppy
    - Formal argument names are API-level artifacts to C# so Mallard's standard of care must be higher
    - Lean towards .NET conventions for argument names
  - Private/internal methods that are particularly "dangerous" or "unsafe", or subtly so such that those aspects are easy to miss, should probably be named with words like `Dangerous` or `Unsafe`
  - In code that locally works with both the native DuckDB object/value and the corresponding .NET object/value at the same time, name the variable for the former with the `native` prefix
    - The .NET object/value's name does not need a prefix unless confusion results otherwise
  - Overload methods only when the functionality between the overloads is essentially the same
    - … and only the types of values differ, or some overloads just supply defaults for certain arguments.
    - Variations of some functionality (e.g. table-based append versus query-based append) should have distinct method names
    - Rationale: many languages don't allow overloading precisely because it can make the code difficult to understand; the types of arguments may not be obvious on a quick glance

## API/ABI compatibility

  - Structure layouts and enumerations imported from DuckDB, where DuckDB's C API necessarily guarantees ABI compatibility are also okay to expose directly as the C# public API
    - Doing so simplifies the code and avoids wasteful conversion functions
  - Correctness of the .NET code should not require the platform be 64-bit even if DuckDB currently, mostly does
    - A few people might want to try to run DuckDB on "embedded" 32-bit hosts
    - Rationale: assuming 64-bit generally does not make code for API *bindings* much faster or easier to understand
    - Mostly, this means 64-bit variables shared between multiple threads should not be assumed to be atomically read/written unless interlocked instructions are consistently used, or they are the same size as the platform's pointer values
    - Code patterns that are (merely) relatively slow on 32-bit are acceptable though
  - Mallard is concerned with C# clients first, then F# second
    - Other .NET languages are too niche, considering the compromises required (even Microsoft only cares about C# and F# nowadays)
    - In particular, Mallard assumes unrestricted ability to consume `ref struct`, extension methods, generics

## Type design

  - Follow the standard advice of "prefer composition over inheritance"
    - Though in C#, because composition of heap-allocated *objects* incurs an extra cost of memory indirection, we do sometimes use inheritance purely as an implementation technique
  - Conservatively make any classes that have no use for inheritance as `sealed`
    - Remember that removing `sealed` to an existing class is an ABI/API-compatible change in .NET but adding `sealed` is not
  - `struct` or `ref struct` is preferred over `class` (.NET reference types) for frequently allocated values
    - … to reduce pressure on the GC allocator and algorithms
  - We do not introduce unnecessary types for the sake of a "consistent type hierarchy" in mapping DuckDB's SQL types
    - Thus, primitive types are represented by `typeof(int)`, `typeof(double)`, etc.
    - We do introduce new types for DuckDB primitive types where:
      - the range of the DuckDB type differs from the approximate equivalent in .NET (e.g. `DuckDbDate` versus `DateOnly`)
      - the ABI representation of the DuckDB type differs from the approximate equivalent in .NET, making the .NET type unsuitable for direct reads from DuckDB vectors
    - As much as possible, it should be possible to use .NET types to read/store data, so that DuckDB-specific types are opt-in
    - Rationale: client code generally does not want gratuitous dependencies on Mallard or even DuckDB; database I/O may only be small part of the code sitting alongside (business) logic
  - DuckDB's type system is a fixed given, but users may want to represent data in ways other than the standard .NET types
    - There might not even be a single best way to map a DuckDB type to a .NET type, in particular for complex types like lists
    - Therefore, Mallard's API should be designed so that the user can extend the type-mapping to custom types
      - That may occur dynamically (dictionaries of custom conversion functions) or statically (extension methods)
  - If a design is both inefficient and difficult to use at the same time, that may hint at something being wrong
    - ADO.NET is arguably that but we do have to implement it for compatibility, obviously
  - Prefer factory methods over constructors for the public API
    - Factory methods are easier to make forward-compatible with later feature additions
    - The *verb* in the method name makes intent more clearer than some of the abstract nouns in class names
    - When constructing object B (e.g. `DuckDbStatement`) from "parent" object A (e.g. `DuckDbConnection`), object B often needs to read private members of object A initially.  A factory method from object A can directly pass the needed members as arguments to the `internal` constructor of object B.  If object B is created through a public constructor, then that constructor would need object A's members be made `internal` instead of `private`, which opens up the guts of object A too much to the rest of the library.

## Memory- and thread-safety

  - Our goal is to avoid all run-time "memory/process corruption"
    - even if the public API is misused (accidentally or intentionally)
    - … assuming the user is not misusing unsafe/dangerous methods elsewhere (e.g. from `System.Runtime.CompilerServices.Unsafe`, etc.)
    - (This is the same design choice made by Microsoft in the standard .NET class library)
  - Use standard .NET patterns like `Span` to avoid exposing (dangling) pointers to user code
  - Enforced through an appropriate balance of dynamic (run-time) checks and static (compile-time) checks
    - Dynamic: [Pros] Can almost always be made to work reliably; [Cons] Incurs cost in memory usage and/or running time
    - Static: [Pros] Nearly free of run-time cost, intent and constraints clearly communicated to user through source code; [Cons] API shapes may become awkward and difficult to use, may prevent legitimate usage patterns that cannot be statically be proven safe
  - Prefer compile-time checks for frequently invoked functionality but the individual instances have a short lifetime in the code, e.g. the values to be added to the database which may number in the thousands or millions
  - Prefer dynamic checks for session objects and other objects that are few in number but persist longer at run-time
  - Wrappers for DuckDB "objects" (i.e. types that involve pointers or heap-allocated memory) can be put in two buckets:
    - Safe for multi-thread access: safety could be ensured through reference counts or locks either in our .NET code or in DuckDB internally
    - Safe for single-thread access: safety enforced at compile-time (`ref struct`), or through some lock or atomic instruction
  - Prefer designs where key data is made immutable to simplify implementation of memory- and thread- safety
    - However, note that C/C++ objects that are "immutable" in the common sense of the word are still not thread-safe for *destruction*, while the .NET user can attempt to call `IDisposable.Dispose` while another thread is still reading from the object — which we still have to guard against
  - Mallard's API should not require the user to explicitly lock objects
    - Modern .NET code can make heavy use of the thread pool and be aggressively asynchronous
    - The thread that is manipulating a (long-running) object may be switched out for another one when any method exits back to user code
  - Another technique to enforce safety is to split some part of an object into a "pure" .NET type, that involves no direct or indirect pointers to native DuckDB objects
    - Even if the user misuses the "pure" .NET type it should not crash
  - No strict requirement to "fail fast" on API misuse or exactly identify the cause
    - But detecting the error as soon and as precisely as possible obviously makes the software more friendly
    - In practice we balance against the run-time cost of checks
  - .NET `struct` types always have the potential to be zero-initialized (by the run-time) even if that does not represent semantically-valid state
    - Operations on such structs should be coded such that exceptions are triggered, implicitly or otherwise, on such zero-initialized invalid state
    - Example: a method may allow the .NET run-time to (freely) trigger `NullReferenceException` when working on a zero-initialized invalid instance
  - Be also aware that .NET `struct` instances may tear when they are inside a heap-allocated object and the user could conceivably manipulate it from multiple threads
    - Diagnosing this issue is not required if memory- and thread- safety is not violated; its frequency of occurrence does not justify the cost of run-time checks
    - Switch to a design based on objects (reference types) or `ref struct` if tearing would cause memory- or thread-safety violation

## Interop

  - We use `ref` and `out` parameters in C# to represent "pass by reference", not pointers
    - There is a small performance hit if the argument is always a local variable, as it may be pinned needlessly, but it does not seem significant
    - Readability is more important, versus double pointers since many "values" are themselves pointers in the DuckDB C API
  - Use the marshalling features in (source-generator-based) P/Invoke to avoid repeating error-prone marshalling code in core API-binding logic
    - However, sometimes the caller doing "manual" marshalling is appropriate if there are real performance gains
  - Use the same names of identifiers (with previously noted exceptions) for DuckDB C API declarations
    - Facilitates cross-referencing with DuckDB's C/C++ source code and copy-and-paste from `duckdb.h`

## Error handling

  - Propagate all errors from DuckDB faithfully
  - The DuckDB C API tends to diligently check for usage errors (e.g. a null value where a non-null pointer is required)
    - But if in doubt, read its source code to confirm
  - For *usage* errors (not operational ones like I/O), consider checking from the .NET side also before calling the native API (if reasonably cheap)
    - For example: null values should ideally throw `ArgumentNullException` instead of `DuckDbException`
    - Take existing practice in .NET as guidance as to what exception to throw
  - Prefer API shapes that statically rule out (certain classes of) usage errors
    - It is acceptable, and preferred, to diverge from the DuckDB native API shape if compile-time safety is gained
    - (On this aspect, our design differs from DuckDB.NET)
  - No resource leaks on any code path (unless the user is doing something very unsafe)

## Dependencies

  - Minimize third-party dependencies
    - Functionality should be split into separate packages (like `Mallard.DataFrames`) when they take dependencies that are not universal
    - Core `Mallard` package only depends on the .NET 10 run-time (other than build-only dependencies)
    - Dependencies in unit tests are obviously more relaxed since they affect only Mallard's developers
  - Consider the quality of the third-party dependency before taking it on

## Code organization

  - Sub-directories in the source code for a package are organized by theme
  - Sub-directories do not correspond to sub-namespaces
    - We prefer a flatter namespace hierarchy; rationale:
      - Few people bother with nested qualified names in their code.  
      - Sub-namespaces are used to hold types that are opt-in, like `DuckDbDate`; users who do not care about such types would not import that sub-namespace and then their code editor's auto-completion would not show those undesired types
      - There is no point in nested namespaces holding types that are almost universally necessary in working with DuckDB
  - Little helper types for a larger type can be placed in the same C# source file as the larger type
    - Can always split the code out later if the helper type grows or its scope expands
  - Split types with large bodies with `partial` definitions across multiple C# source files, organized by theme

## General

  - If a rule appears violated at some place in the codebase, consider it a possible bug, flaw or work-in-progress
    - Do not blindly imitate or copy-and-paste obvious mistakes or oversights
  - Some of the above rules will inevitably conflict depending on the situation: use best common-sense judgement
  - Existing practice across the .NET ecosystem should not imitated blindly but should be ranked
    - .NET has a long history and there are a lot of outdated and inadvisable patterns in code that is nevertheless commonly used
    - There is a long tail of .NET code whose standards are not as high or consistent as Microsoft or major third parties
    - Examples: classes that are mutation-heavy and require explicit user locking for thread safety, Java-esque "enterprisey" class/interface hierarchies
    - Modern data-analytical workloads lean towards functional coding styles, and parallelism being a first-class concern

