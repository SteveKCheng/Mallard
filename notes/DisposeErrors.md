# What to do with errors in disposing a .NET object?

## Developer's query to Gemini

I have a question about the design of .NET classes, specifically about `IDisposable.Dispose`.

Should `Dispose` throw exceptions when errors occur during disposal?

For avoidance of ambiguity, let me state first that "disposing an object that has already been disposed" is *not* an error for the purposes of this discussion.  (That's pretty clear from existing practice as well as formal guidance from Microsoft.)

The subject for this question I have in mind is a class that implements some kind of "remote file", i.e. an object on a remote filesystem, that aggressively buffers and transfers data in the background.

I believe, on good evidence, that the general understanding of `Dispose` on file-like objects is that the file gets "closed".  Consider this official Microsoft documentation: [.NET / API browser / Stream.Close Method](https://learn.microsoft.com/en-us/dotnet/api/system.io.stream.close?view=net-10.0):

<blockquote>
Closes the current stream and releases any resources (such as sockets and file handles) associated with the current stream. Instead of calling this method, ensure that the stream is properly disposed.

**Remarks**

This method calls Dispose, specifying true to release all resources. You do not have to specifically call the Close method. Instead, ensure that every Stream object is properly disposed. You can declare Stream objects within a using block (or Using block in Visual Basic) to ensure that the stream and all of its resources are disposed, or you can explicitly call the Dispose method.
</blockquote>

On the other hand, this method `Close` is declared as `virtual` so individual implementations could conceivably make it behave differently than `Dispose`.  Yet, in [.NET / API browser / Stream.Dispose Method](https://learn.microsoft.com/en-us/dotnet/api/system.io.stream.dispose?view=net-10.0) we find:

<blockquote>

**Notes to Inheritors**

Place all cleanup logic for your stream object in Dispose(Boolean). Do not override Close().

Note that because of backward compatibility requirements, this method's implementation differs from the recommended guidance for the Dispose pattern. This method calls Close(), which then calls Dispose(Boolean).
</blockquote>

In any case, what I want to say is that making "Dispose" be non-synonymous with "Close" violates the principle of least surprise.

So, what happens if the closing of a remote file means flushing pending data --- and that data transfer fails?  Surely a robust client application would want to know, so it could retry the operation, or inform the user and then the user can investigate or try re-doing the operation.  So errors should not be swallowed.

I have a second argument for my position.  In recent .NET versions there is `IAsyncDisposable.DisposeAsync`.  Now, asynchronous methods are typically used for I/O (although they can also be profitably used for CPU-bound tasks), and I think everybody intuitively knows that any I/O operation can fail.  If anything, many asynchronous methods take `CancellationToken` and asynchronous methods should throw exceptions merely when the `CancellationToken` has been cancelled.  Though `IAsyncDisposable.DisposeAsync` does not take `CancellationToken`, I think that it is intuitive it should do the same thing as `IDisposable.Dispose`, only asynchronously, and both methods are expected to do "finishing I/O" (e.g. flushing buffers) as part of disposing.  For stream I/O especially, we do not expect clients to have explicit calls to "Close"; instead the stream gets closed implicitly when code exits the `using` block in C# (that typically bounds the stream object).

I will note that in this respect, "Dispose" is different from the concept of "destruction" in C++ which typically also de-allocates *all* the memory used to manage the object.  To avoid resource leaks, C++ generally wants destructors for all objects to complete. (Of course, this can't be guaranteed if one has reference cycles for reference-counted objects, but we'll leave that caveat aside.)  So destructors are generally considered to not throw any exceptions (`noexcept` in C++).  

And trying to override that C++ default and throw exceptions from a destructor almost always opens a can of worms and generally advisable: in the C++ code I've written, I've only found a use for that in "scope" objects that ensures some mutable state is temporarily manipulated within the scope gets automatically reverted/restored at scope exit, trying to minimize the chance that the coder would forget.  And even then the order of destruction of objects can get too subtle.

I'd say though that C# is different in that all classes managing native resources should already have finalizers that try to clean up even if the user forgets to `Dispose`.  So, while `Dispose` throwing exceptions in C# can still give rise to subtle ordering problems, I'd argue the consequences are less severe.

How should we think about this issue?  For your answer, please look at guidance and existing practice from battle-tested, reputable code bases / standards like .NET itself.

## Response from Gemini

In .NET, the question of whether `IDisposable.Dispose` (and `IAsyncDisposable.DisposeAsync`) should throw exceptions is one of the most famous design tensions in the framework.

The short answer is: **As a general rule, `Dispose` should NOT throw exceptions.** **However, write-buffered I/O types (like `Stream` and `FileStream`) represent a well-known exception to this rule**, where data integrity takes precedence over clean disposal semantics.

### 1. The General Rule: Avoid Throwing from `Dispose`

According to Microsoft’s official **Framework Design Guidelines** (by Krzysztof Cwalina and Brad Abrams):

> **AVOID** throwing an exception from within `Dispose(bool)` except under critical situations where the containing process has been corrupted.

#### Why `Dispose` avoids exceptions

1. **Exception Masking in `using` / `finally` Blocks**
In C#, a `using` statement translates to a `try...finally` block:
```csharp
var file = new RemoteFileStream(...);
try
{
    file.Write(data); // <--- Suppose this throws SocketException!
}
finally
{
    file.Dispose();   // <--- If this ALSO throws IOException, 
                      // the SocketException is SILENTLY LOST!
}

```
If code inside the `try` block throws an exception, C# enters the `finally` block. If `Dispose()` throws a second exception inside `finally`, **the second exception replaces and masks the original exception**. You lose the stack trace and root cause of the primary failure.

2. **Cascade Failures in Multi-Resource Cleanup**.
If a component cleans up multiple resources in its `Dispose` method, an exception thrown by the first resource’s `Dispose()` prevents cleanup logic for remaining resources from ever executing, causing memory or handle leaks.

3. **The WCF Anti-Pattern**.
The most famous violation of this guideline in .NET history occurred in **Windows Communication Foundation (WCF)**. WCF's client channels threw `CommunicationException` inside `Dispose()` if the channel was in a faulted state. This made WCF clients unsafe to use inside C# `using` blocks. Microsoft eventually had to document a workaround advising developers **not** to use `using` statements with WCF clients, citing it as an API design flaw.

### 2. The Exception to the Rule: Streams and Write-Buffers

Despite the general guideline, **`System.IO.Stream` and its subclasses (like `FileStream` and `CryptoStream`) intentionally throw exceptions during `Dispose()`**.

Why does `Stream.Dispose()` break the rule?

* **Data Integrity over Resource Releasing:** When a stream buffers writes, calling `Dispose()` or `Close()` invokes `Flush()`. If flushing fails (e.g., disk full, remote filesystem drop, network drop), ignoring the error during disposal would mean **silent data loss**.
* In write-buffered streams, completing the write operation is part of the object’s contract. Silently swallowing a failed write during `Dispose()` violates the contract more severely than throwing an exception from a `finally` block.

### 3. Addressing Your Specific Arguments

#### Finalizers & C++ vs. C#

While it is true that C# managed finalization differs from C++ deterministic destruction:

* **Finalizers MUST NEVER throw exceptions:** In .NET (Core / .NET 5+), throwing an unhandled exception inside a finalizer (`Dispose(false)`) will **terminate the process immediately**. Framework guidelines mandate: *If `disposing` is `false`, never throw an exception*.
* **Exception Masking is still severe in C#:** Even though garbage collection prevents memory corruption from skipped destructors, masking the primary exception inside `using` / `await using` remains a major source of hidden production bugs in .NET.

#### `IAsyncDisposable` and Async I/O

You correctly noted that modern remote streams should prefer `IAsyncDisposable.DisposeAsync()`. However, `await using` uses the exact same `try...finally` lowering under the hood. Throwing inside `DisposeAsync()` masks exceptions thrown inside the `await using` block in the exact same manner.

### 4. Recommended Architectural Designs for a "Remote File" Class

When designing a remote file stream, you have three battle-tested design patterns to choose from:

#### Pattern A: Standard `Stream` / `IAsyncDisposable` (Flush on Dispose)

If your class inherits from `System.IO.Stream` or acts as a direct stream wrapper, follow .NET Stream conventions:

* **Behavior:** `Dispose()` / `DisposeAsync()` attempts to flush pending data. If the remote write fails, throw an `IOException` or `SocketException`.
* **Guidance to Consumers:** Document that consumers must explicitly call `await stream.FlushAsync()` before exiting the `using` block if they want to handle flush errors explicitly without risking exception masking.

#### Pattern B: Two-Stage Completion (Explicit `CompleteAsync` + `DisposeAsync`)

Used by modern high-performance .NET libraries like **`System.IO.Pipelines`** (`PipeWriter`):

* **Behavior:** Separate the *data completion* step from the *resource release* step.
* Call `await file.CompleteAsync()` or `await file.FlushAsync()` to finalize the write and flush data. **This method throws on I/O error.**
* Call `DisposeAsync()` strictly to release sockets, handles, and memory. **`DisposeAsync()` does not throw.**


* **Advantage:** Prevents exception masking entirely. The consumer explicitly handles data persistence errors in main-line execution, while `using` strictly guarantees safe resource cleanup.

```csharp
await using var remoteFile = new RemoteFileStream(path);
await remoteFile.WriteAsync(buffer);

// Explicit completion phase handles write/network errors
await remoteFile.CompleteAsync(); 

// DisposeAsync() runs at end of scope safely without throwing

```

#### Pattern C: Safe/Defensive Disposal (Context-Aware Dispose)

If `Dispose()` attempts a final flush, check whether an exception is already in flight (or whether the object is in a corrupted state):

* If `Dispose()` is called during a normal execution exit and flushing fails $\rightarrow$ throw the I/O exception.
* If the object was marked as aborted/faulted due to an earlier error during stream operations $\rightarrow$ suppress the flush error during `Dispose()` to avoid masking the original exception.

### Summary Checklist

| Context | Recommended `Dispose` Behavior |
| --- | --- |
| **General Objects / Managed Resources** | **Never throw.** Swallow non-critical cleanup errors or log them. |
| **Write-Buffered Streams (`Stream`, `FileStream`)** | **Allowed to throw** on `Flush()` failure to prevent silent data loss. |
| **Finalizer Path (`Dispose(false)`)** | **MUST NEVER throw** under any circumstance. |
| **Modern Async I/O Design** | Prefer **Two-Stage Completion** (`CompleteAsync` + `DisposeAsync`) to separate data persistence from resource cleanup. |
