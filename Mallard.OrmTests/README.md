ORM Integration tests for Mallard
=================================

This test program uses a hybrid framework:

  * The execution engine is [TUnit](https://tunit.dev/),
  * but the assertions are written in [xUnit v3](https://xunit.net/) syntax.

See [the description for Mallard.Tests](../Mallard.Tests/README.md) for details.

The integration tests of ORM (Object-Relational Mapping) is separated out from 
the main unit test program because:

  - The integration tests use non-trivial external dependencies, such as Dapper.
  - Those external dependencies typically require reflection 
    and so might not be compatible with AOT (ahead-of-time compilation).

A few C# source files are shared between the integration tests and the unit tests.

