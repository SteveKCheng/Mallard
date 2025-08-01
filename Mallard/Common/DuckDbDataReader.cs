using System;
using System.Collections;
using System.Collections.Immutable;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace Mallard;

/// <summary>
/// ADO.NET-compatible reader for the results of a query in DuckDB.
/// </summary>
public sealed class DuckDbDataReader : DbDataReader
{
    #region Construction

    private readonly DuckDbResult _queryResults;

    internal DuckDbDataReader(DuckDbResult queryResults)
    {
        _queryResults = queryResults;

        // Must cache this right away because queryResults does not allow multi-thread access
        _numberOfRowsChanged = queryResults.GetNumberOfChangedRows(out _hasResultRows);
    }

    #endregion

    #region Number of rows affected

    private readonly bool _hasResultRows;
    private readonly long _numberOfRowsChanged;

    /// <inheritdoc />
    public override bool HasRows => _hasResultRows;

    /// <inheritdoc />
    public override int RecordsAffected => int.CreateSaturating(_numberOfRowsChanged);

    #endregion

    #region Closing the reader

    private bool _isClosed;

    /// <inheritdoc />
    public override bool IsClosed => _isClosed;

    /// <inheritdoc />
    public override void Close()
    {
        _isClosed = true;
        _currentChunk = null;
    }

    #endregion

    #region Private helpers

    private DuckDbResultChunk GetCurrentChunk()
        => _currentChunk ?? throw new InvalidOperationException(
            "There are no more chunks, or this data reader has not been started, or has been closed. ");

    private DuckDbVectorReader<T> GetVectorReader<T>(int columnIndex)
        => GetCurrentChunk().UnsafeGetColumnReader<T>(columnIndex);

    #endregion

    #region Retrieval of field values, generically

    public override T GetFieldValue<T>(int ordinal)
    {
        var reader = GetVectorReader<T>(ordinal);
        return reader.GetItem(_currentChunkRow);

        /*
        ref readonly Column column = ref _currentColumns[ordinal];
        if (typeof(T) == column.Converter.TargetType)
        {
            return column.Converter.Convert<T>(column.Vector, _currentChunkRow, requireValid: true)!;
        }
        else
        {
            object v = column.Converter.Convert<object>(column.Vector, _currentChunkRow, requireValid: true)!;
            if (typeof(T) != typeof(object))
                v = Convert.ChangeType(v, typeof(T));

            return (T)v;
        }*/
    }

    #endregion

    #region Retrieval of field values, with boxing

    /// <inheritdoc />
    public override object this[int ordinal] => GetValue(ordinal);

    /// <inheritdoc />
    public override object this[string name] => GetValue(GetOrdinal(name));

    /// <inheritdoc />
    public override object GetValue(int ordinal)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc />
    public override int GetValues(object[] values)
    {
        var numColumns = Math.Min(_queryResults.ColumnCount, values.Length);
        for (int columnIndex = 0; columnIndex < numColumns; ++columnIndex)
            values[columnIndex] = GetValue(columnIndex);

        return numColumns;
    }

    #endregion

    #region Retrieval of blobs, through user-provided buffer

    /// <inheritdoc />
    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc />
    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        throw new NotImplementedException();
    }

    #endregion

    #region Miscellaneous

    /// <inheritdoc />
    public override IEnumerator GetEnumerator() => new DbEnumerator(this);

    /// <inheritdoc />
    public override int Depth => 0;

    #endregion

    #region Retrieving field values for primitive types, non-generically

    /// <inheritdoc />
    public override bool GetBoolean(int ordinal) => GetFieldValue<bool>(ordinal);

    /// <inheritdoc />
    public override byte GetByte(int ordinal) => GetFieldValue<byte>(ordinal);

    /// <inheritdoc />
    public override char GetChar(int ordinal) => GetFieldValue<char>(ordinal);

    /// <inheritdoc />
    public override DateTime GetDateTime(int ordinal) => GetFieldValue<DateTime>(ordinal);

    /// <inheritdoc />
    public override decimal GetDecimal(int ordinal) => GetFieldValue<Decimal>(ordinal);

    /// <inheritdoc />
    public override double GetDouble(int ordinal) => GetFieldValue<double>(ordinal);

    /// <inheritdoc />
    public override float GetFloat(int ordinal) => GetFieldValue<float>(ordinal);

    /// <inheritdoc />
    public override Guid GetGuid(int ordinal) => GetFieldValue<Guid>(ordinal);

    /// <inheritdoc />
    public override short GetInt16(int ordinal) => GetFieldValue<short>(ordinal);

    /// <inheritdoc />
    public override int GetInt32(int ordinal) => GetFieldValue<int>(ordinal);

    /// <inheritdoc />
    public override long GetInt64(int ordinal) => GetFieldValue<long>(ordinal);

    /// <inheritdoc />
    public override string GetString(int ordinal) => GetFieldValue<string>(ordinal);

    #endregion

    #region Queries of column information

    /// <inheritdoc />
    public override int FieldCount => _queryResults.ColumnCount;

    /// <inheritdoc />
    public override string GetName(int ordinal)
    {
        return _queryResults.GetColumnName(ordinal);
    }

    /// <summary>
    /// Dictionary to look up the column index for a column name.
    /// </summary>
    private ImmutableDictionary<string, int>? _fieldNamesMap;

    /// <inheritdoc />
    public override int GetOrdinal(string name)
    {
        // Linear search if there are 8 columns or fewer
        if (_queryResults.ColumnCount < 8)
        {
            for (int columnIndex = 0; columnIndex < _queryResults.ColumnCount; ++columnIndex)
            {
                if (_queryResults.GetColumnName(columnIndex) == name)
                    return columnIndex;
            }
        }

        // Construct a dictionary to look up names
        else
        {
            if (_fieldNamesMap == null)
            {
                var builder = ImmutableDictionary.CreateBuilder<string, int>();
                for (int columnIndex = 0; columnIndex < _queryResults.ColumnCount; ++columnIndex)
                    builder.Add(_queryResults.GetColumnName(columnIndex), columnIndex);
                _fieldNamesMap = builder.ToImmutable();
            }

            if (_fieldNamesMap.TryGetValue(name, out int foundIndex))
                return foundIndex;
        }

        throw new IndexOutOfRangeException("The name specified is not a valid column name in this result set. ");
    }

    /// <inheritdoc />
    public override string GetDataTypeName(int ordinal)
    {
        throw new NotImplementedException();
    }

    [return: DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type GetFieldType(int ordinal)
    {
        throw new NotImplementedException();
    }

    #endregion

    #region Null handling

    /// <inheritdoc />
    public override bool IsDBNull(int ordinal)
        => GetCurrentChunk().UnsafeGetColumnVector(ordinal).IsItemValid(_currentChunkRow);

    #endregion

    #region Advancing this reader

    private int _currentChunkRow;
    private DuckDbResultChunk? _currentChunk;

    /// <inheritdoc />
    public override bool NextResult()
    {
        return false;
    }

    /// <inheritdoc />
    public override bool Read()
    {
        ObjectDisposedException.ThrowIf(_isClosed, this);

        if (_currentChunk != null)
        {
            if (++_currentChunkRow < _currentChunk.Length)
                return true;

            _currentChunkRow = 0;
        }

        // Fetch the next non-empty chunk.
        // Chunks should not be empty but we will be defensive.
        do
        {
            _currentChunk = _queryResults.FetchNextChunk();
        } while (_currentChunk != null && _currentChunk.Length == 0);

        return (_currentChunk != null);
    }

    #endregion
}
