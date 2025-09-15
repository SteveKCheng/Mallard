using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Mallard;

/// <summary>
/// ADO.NET-compatible collection of parameters for <see cref="DuckDbCommand" />. 
/// </summary>
/// <remarks>
/// <para>
/// This implementation of <see cref="IDataParameterCollection" /> is essentially a plain list
/// of the parameters.  Most validation happens when the <see cref="DuckDbCommand" />
/// is executed, not when a parameter is added to this collection.
/// </para>
/// <para>
/// It is not recommended to mix positional parameters with named parameters.
/// DuckDB may assign indices to parameters differently than your code.
/// </para>
/// </remarks>
public sealed class DuckDbParameterCollection : IDataParameterCollection, IList<DbParameter>
{
    /// <summary>
    /// Parameters accessed by index.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Because <see cref="IDbDataParameter.ParameterName" /> is a mutable property that
    /// can be changed at any time by the user (a bad design from the "enterprisey" code
    /// patterns from the beginning of the millenium)
    /// it is hard to cache the parameters keyed by name.  In
    /// other implementations, such as <see cref="System.Data.Odbc.OdbcParameterCollection" />
    /// and <see cref="Microsoft.Data.SqlClient.SqlParameterCollection" />, when the user
    /// asks to look up a parameter by name, linear search is performed.  We follow their
    /// precedent.  It is unlikely that client code using ADO.NET depends on the name
    /// look-up being O(1).
    /// </para>
    /// </remarks>
    private readonly List<DbParameter> _items = new List<DbParameter>();

    IEnumerator<DbParameter> IEnumerable<DbParameter>.GetEnumerator() => _items.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => _items.GetEnumerator();

    public void CopyTo(Array array, int index) => ((IList)_items).CopyTo(array, index);

    public bool Remove(DbParameter item) => _items.Remove(item);

    public int Count => _items.Count;
    public bool IsSynchronized => false;
    public object SyncRoot => _items;

    public void Add(DbParameter value) => _items.Add(ValidateParameter(value));

    int IList.Add(object? value)
    {
        var index = _items.Count;
        _items.Add(ValidateParameter(value));
        return index;
    }

    public void Clear() => _items.Clear();

    public bool Contains(DbParameter value) => _items.Contains(value);
    
    public void CopyTo(DbParameter[] array, int arrayIndex) => _items.CopyTo(array, arrayIndex);

    bool IList.Contains(object? value) => value is DbParameter p && Contains(p);

    public int IndexOf(DbParameter value) => _items.IndexOf(value);

    public void Insert(int index, DbParameter item) => _items.Insert(index, ValidateParameter(item));

    int IList.IndexOf(object? value) => value is DbParameter p ? IndexOf(p) : -1; 

    void IList.Insert(int index, object? value) => _items.Insert(index, ValidateParameter(value));

    void IList.Remove(object? value)
    {
        if (value is DbParameter p)
            _items.Remove(p);
    }

    public void RemoveAt(int index) => _items.RemoveAt(index);

    public bool IsFixedSize => false;
    public bool IsReadOnly => false;

    private DbParameter ValidateParameter(DbParameter item, [CallerArgumentExpression(nameof(item))] string? paramName = null)
    {
        ArgumentNullException.ThrowIfNull(item, paramName);
        return item;
    }

    private DbParameter ValidateParameter(object? item,
                                          [CallerArgumentExpression(nameof(item))] string? paramName = null)
    {
        ArgumentNullException.ThrowIfNull(item, paramName);
        if (item is not DbParameter p)
            throw new ArgumentException($"Cannot insert anything but a {nameof(DbParameter)} object into DuckDbParameterCollection. ");

        return p;
    }

    public DbParameter this[int index]
    {
        get => _items[index];
        set => _items[index] = ValidateParameter(value);
    }

    private static bool IsNamedParameter(string parameterName)
        => !string.IsNullOrEmpty(parameterName);

    object? IList.this[int index]
    {
        get => _items[index];
        set => _items[index] = ValidateParameter(value);
    }

    public bool Contains(string parameterName)
        => IndexOf(parameterName) >= 0;

    public int IndexOf(string parameterName)
        => GetIndexForParameterName(parameterName, false);

    public void RemoveAt(string parameterName)
        => _items.RemoveAt(GetIndexForParameterName(parameterName));

    private int GetIndexForParameterName(string parameterName, bool throwIfNotFound = true)
    {
        for (int i = 0; i < _items.Count; ++i)
        {
            if (_items[i].ParameterName == parameterName)
                return i;
        }

        if (throwIfNotFound)
        {
            throw new KeyNotFoundException(
                $"There is no parameter with the given name in this collection. Key: {parameterName}");
        }
        
        return -1;
    }

    /// <summary>
    /// Set (the value of) a parameter keyed by its name.
    /// </summary>
    /// <param name="parameterName">
    /// The name of the parameter to set.  A parameter object with this name must have been
    /// added earlier to this collection.
    /// </param>
    /// <remarks>
    /// <para>
    /// A named parameter has an index no different than unnamed parameters.  Calling the
    /// setter on this property will replace the parameter object at the index
    /// where a parameter with the given name lives.  The replacement parameter object
    /// may have a different name.  (This behavior is consistent with other implementations
    /// of <see cref="IDataParameterCollection" />.)
    /// </para>
    /// <para>
    /// If the new parameter object being set is unnamed, its name will automatically be
    /// set to <paramref name="parameterName" />.
    /// </para>
    /// </remarks>
    public DbParameter this[string parameterName]
    {
        get => _items[GetIndexForParameterName(parameterName)];
        set
        {
            var index = GetIndexForParameterName(parameterName); 
            ValidateParameter(value);
            
            var n = value.ParameterName;
            if (string.IsNullOrEmpty(n))
                value.ParameterName = parameterName;
            
            _items[index] = value;
        }
    }

    object IDataParameterCollection.this[string parameterName]
    {
        get => this[parameterName];
        set => this[parameterName] = ValidateParameter(value);
    }
}
