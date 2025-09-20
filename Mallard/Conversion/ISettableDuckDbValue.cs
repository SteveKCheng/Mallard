using Mallard.Interop;

namespace Mallard;

/// <summary>
/// Accepts a value from .NET code to pass into DuckDB. 
/// </summary>
/// <remarks>
/// <para>
/// There are a several kinds of objects in DuckDB that take in values that may be
/// of any of the types supported in its SQL dialect, including but not limited to
/// integers, date/time, strings, and blobs.  Ultimately there needs to be conversions
/// from .NET values to their native representations under DuckDB.  (This conversion
/// is in the opposite direction of the conversion that happens when retrieving values/results
/// from DuckDB.)
/// </para>
/// <para>
/// To facilitate discovery of the .NET types that are supported for conversion
/// (into DuckDB), one method to set a value, for each supported data type,
/// is defined in <see cref="DuckDbValue" /> as an extension method.  Thus the static
/// type is visible statically (instead of being erased as <see cref="object" />
/// or hidden behind a unconstrained generic parameter).  The compiler can apply
/// implicit conversions for the supported data types. 
/// </para>
/// <para>
/// The receiver object implements this interface, which has no public methods.
/// All the value-setting operations are implemented in <see cref="DuckDbValue" />.
/// This simplifies the implementation, and at the same time, allows the user to
/// define conversions from other data types on almost the same footing as built-in types
/// supported by Mallard. 
/// </para>
/// </remarks>
public interface ISettableDuckDbValue
{
    /// <summary>
    /// Accept a value which has been packaged into DuckDB's generic value wrapper.
    /// </summary>
    /// <param name="nativeValue">
    /// The native object created to represent the (original) input value.
    /// This method takes ownership of it (whether this method succeeds or fails with
    /// an exception).  
    /// </param>
    internal unsafe void SetNativeValue(_duckdb_value* nativeValue);
}
