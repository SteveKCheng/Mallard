using System;
using Mallard.Interop;

namespace Mallard;

public unsafe partial class DuckDbAppender : IDisposable
{
    private _duckdb_appender* _nativeObj;
    private Barricade _barricade;
    private uint _sequenceCounter;
    
    internal DuckDbAppender(_duckdb_connection* nativeConn,
                            string catalogName,
                            string schemaName,
                            string tableName)
    {
        var status = NativeMethods.duckdb_appender_create_ext(nativeConn, 
                                                     catalogName, 
                                                     schemaName, 
                                                     tableName, 
                                                     out var nativeObj);
        try
        {
            if (status == duckdb_state.DuckDBError)
            {
                throw new DuckDbException("Failed to create appender. ");
            }
        }
        catch
        {
            NativeMethods.duckdb_appender_destroy(ref nativeObj);
            throw;
        }

        _nativeObj = nativeObj;
    }

    private void DisposeImpl(bool disposing)
    {
        if (!_barricade.PrepareToDisposeOwner())
            return;

        NativeMethods.duckdb_appender_destroy(ref _nativeObj);
    }

    ~DuckDbAppender()
    {
        DisposeImpl(disposing: false);
    }

    public void Dispose()
    {
        DisposeImpl(disposing: true);
        GC.SuppressFinalize(this);
    }

    public ItemState Append() => new ItemState(this);

    private void ThrowOnAppendFailure(duckdb_state status)
    {
        if (status != duckdb_state.DuckDBSuccess)
            throw new DuckDbException("Failed to append value. ");
    }

    public void FinishRow()
    {
        using var _ = _barricade.EnterScope(this);
        NativeMethods.duckdb_appender_end_row(_nativeObj);
    }
}
