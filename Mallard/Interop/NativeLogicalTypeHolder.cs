using Mallard.Interop;

namespace Mallard;

internal unsafe ref struct NativeLogicalTypeHolder(_duckdb_logical_type* nativeType)
{
    private _duckdb_logical_type* _nativeType = nativeType;

    public readonly _duckdb_logical_type* NativeHandle => _nativeType;

    public void Dispose()
    {
        if (_nativeType != null)
            NativeMethods.duckdb_destroy_logical_type(ref _nativeType);
    }
}
