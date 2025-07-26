using Mallard.C_API;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;

namespace Mallard;

/// <summary>
/// Retrieves the names of the members in a DuckDB enumeration.
/// </summary>
public unsafe sealed class DuckDbEnumDictionary : IReadOnlyDictionary<uint, string>, IDisposable
{
    private _duckdb_logical_type* _nativeType;
    private readonly uint _totalEnumMembers;
    private uint _countEvaluatedMembers;
    private readonly string?[]? _memberNames;
    private HandleRefCount _refCount;

    /// <inheritdoc cref="IReadOnlyDictionary{TKey, TValue}.Keys" />
    public IEnumerable<uint> Keys
    {
        get
        {
            for (uint i = 0; i < _totalEnumMembers; ++i)
                yield return i;
        }
    }

    /// <inheritdoc cref="IReadOnlyDictionary{TKey, TValue}.Values" />
    public IEnumerable<string> Values
    {
        get
        {
            for (uint i = 0; i < _totalEnumMembers; ++i)
                yield return GetMemberName(i);
        }
    }

    /// <inheritdoc cref="IReadOnlyCollection{T}.Count" />
    public int Count => (int)_totalEnumMembers;

    /// <summary>
    /// Get the string corresponding to an enumeration value.
    /// </summary>
    /// <param name="key">The enumeration value. </param>
    /// <returns>The string associated to the enumeration value when the enumeration
    /// was defined in DuckDB. </returns>
    public string this[uint key] => GetMemberName(key);

    internal DuckDbEnumDictionary(ref _duckdb_logical_type* nativeType)
    {
        _totalEnumMembers = NativeMethods.duckdb_enum_dictionary_size(nativeType);

        if (_totalEnumMembers <= ushort.MaxValue + 1)
            _memberNames = new string?[_totalEnumMembers];

        _nativeType = nativeType;
        nativeType = default;
    }

    internal static DuckDbEnumDictionary CreateFromContext(ref readonly ConverterCreationContext context)
    {
        var nativeType = context.GetNativeLogicalType();
        if (nativeType == null)
            throw new DuckDbException("Could not query the logical type of a vector from DuckDB. ");

        try
        {
            return new DuckDbEnumDictionary(ref nativeType);
        }
        catch
        {
            NativeMethods.duckdb_destroy_logical_type(ref nativeType);
            throw;
        }
    }

    private void VerifyIndex(uint index)
    {
        if (index >= _totalEnumMembers)
            throw new IndexOutOfRangeException("Given index is not valid for this DuckDB enumeration. ");
    }

    private string GetMemberName(uint index)
    {
        VerifyIndex(index);

        string? name = _memberNames?[index];

        if (name == null)
        {
            using (var _ = _refCount.EnterScope(this))
            {
                name = NativeMethods.duckdb_enum_dictionary_value(_nativeType, index);
            }

            if (_memberNames != null)
            {
                // Ensure only the string from the first look-up of this enumeration value
                // is stored, even though look-ups may race.  Then, downstream consumers
                // could use reference equality to compare values.
                var oldName = Interlocked.CompareExchange(ref _memberNames[index], name, null);
                if (oldName != null)
                    name = oldName;

                // Dispose native object early if we already got all the strings.
                // This class is often used in contexts where explicit disposal is not convenient.
                else if (Interlocked.Increment(ref _countEvaluatedMembers) == _totalEnumMembers)
                    Dispose();
            }
        }

        return name;
    }

    private void DisposeImpl(bool disposing)
    {
        if (!_refCount.PrepareToDisposeOwner())
            return;
        NativeMethods.duckdb_destroy_logical_type(ref _nativeType);
    }

    /// <summary>
    /// Disposes the resource held from the natice DuckDB library, if any exists
    /// currently, used to query the enumeration members.
    /// </summary>
    /// <remarks>
    /// After calling this method, this instance should not be used again.
    /// </remarks>
    public void Dispose()
    {
        DisposeImpl(disposing: true);
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc cref="IReadOnlyDictionary{TKey, TValue}.ContainsKey(TKey)" />
    public bool ContainsKey(uint key)
        => key < _totalEnumMembers;

    /// <inheritdoc cref="IReadOnlyDictionary{TKey, TValue}.TryGetValue(TKey, out TValue)" />
    public bool TryGetValue(uint key, [MaybeNullWhen(false)] out string value)
    {
        if (key >= _totalEnumMembers)
        {
            value = null;
            return false;
        }

        value = GetMemberName(key);
        return true;
    }

    /// <inheritdoc cref="IEnumerable{KeyValuePair{TKey, TValue}}.GetEnumerator" />
    public IEnumerator<KeyValuePair<uint, string>> GetEnumerator()
    {
        for(uint i = 0; i < _totalEnumMembers; ++i)
            yield return new KeyValuePair<uint, string>(i, GetMemberName(i));
    }

    /// <inheritdoc cref="IEnumerable.GetEnumerator" />
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    ~DuckDbEnumDictionary()
    {
        DisposeImpl(disposing: false);
    }
}

