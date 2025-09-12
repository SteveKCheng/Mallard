Unit tests for Mallard
======================

This unit-test program uses a hybrid framework:

  * The execution engine is [TUnit](https://tunit.dev/),
  * but the assertions are written in [xUnit v3](https://xunit.net/) syntax.

Originally execution engine was also xUnit v3.  Unfortunately,
[xUnit v3 does not allow AOT execution](https://github.com/xunit/xunit/issues/3154),
but we need to test that in Mallard.  That TUnit happens to be 
faster in some scenarios is just a bonus.

On the other hand, we prefer xUnit's traditional but “no-brainer” syntax
for writing assertions.  TUnit's assertions syntax is verbose
and requires the invoking method be asynchronous, which makes
debugging more difficult and does not work well with local variables
of ``ref struct`` types.  We follow [a suggestion to mix xUnit and TUnit](https://github.com/thomhurst/TUnit/issues/580#issuecomment-2563790241) to get the best of both
worlds.  

