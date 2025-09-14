using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Runtime.InteropServices;

namespace Mallard;

public sealed class DuckDbParameterCollection : IDataParameterCollection
{
    private List<DbParameter> _parameters = new List<DbParameter>();
    private Dictionary<string, int> _nameMap = new Dictionary<string, int>();
    
    public IEnumerator GetEnumerator() => _parameters.GetEnumerator();

    public void CopyTo(Array array, int index)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(index, "index");
        
        if (_parameters.Count > array.Length - index)
        {
            throw new ArgumentException(
                "The number of elements in this collection is greater than " +
                "the number of elements from the specified index to the end of the destination array.");
        }

        for (int j = 0; j < _parameters.Count; ++j)
            array.SetValue(_parameters[j], index + j);
    }

    public int Count => _parameters.Count;
    public bool IsSynchronized => false;
    public object SyncRoot => _parameters;

    public int Add(DbParameter value) => Add(value.ParameterName, value);

    private int Add(string parameterName, DbParameter value)
    {
        int index = _parameters.Count;

        if (parameterName != value.ParameterName)
        {
            throw new ArgumentException(
                "The value of the ParameterName property does not match the name that the parameter is to be added under. "
                + $" Key: {parameterName}; ParameterName: {value.ParameterName}");
        }

        if (!_nameMap.TryAdd(parameterName, index))
        {
            throw new ArgumentException(
                $"A parameter with the same name already exists in the same collection. Key: {parameterName}");
        }
        
        _parameters.Add(value);

        return index;
    }

    int IList.Add(object? value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return Add((DbParameter)value);
    }

    public void Clear()
    {
        _parameters.Clear();
        _nameMap.Clear();
    }

    public bool Contains(DbParameter value)
    {
        int index = IndexOf(value.ParameterName);
        return index >= 0 && _parameters[index] == value;
    }
    
    bool IList.Contains(object? value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return Contains((DbParameter)value);
    }

    public int IndexOf(DbParameter value)
    {
        int index = IndexOf(value.ParameterName);
        return index >= 0 && _parameters[index] == value ? index : -1;
    }

    int IList.IndexOf(object? value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return IndexOf((DbParameter)value);
    }

    public void Insert(int index, object? value)
    {
        throw new NotImplementedException();
    }

    public void Remove(object? value)
    {
        throw new NotImplementedException();
    }

    public void RemoveAt(int index)
    {
        throw new NotImplementedException();
    }

    public bool IsFixedSize => false;
    public bool IsReadOnly => false;

    public DbParameter this[int index]
    {
        get => _parameters[index];
        set
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(index, 0);
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(index, _parameters.Count);

            var newParameterName = value.ParameterName;
            
            // The parameter has a new name not seen before.  It replaces the old name,
            // from the old DbParameter instance at the same index.
            if (!_nameMap.TryGetValue(newParameterName, out int oldIndex))
            {
                var oldParameterName = _parameters[index].ParameterName;
                
                // Need to linearly scan if the user changed the parameter name out from under us
                if (_nameMap.GetValueOrDefault(oldParameterName, -1) != index)
                    oldParameterName = _nameMap.Single(p => p.Value == index).Key;
                
                _nameMap.Add(newParameterName, index);
                _nameMap.Remove(oldParameterName);
            }
            
            // The parameter has been renamed to the same name as another parameter
            // at a different index, which is not allowed.
            else if (oldIndex != index)
            {
                throw new ArgumentException("A parameter with the same name already exists at a different index. ");
            }

            _parameters[index] = value;
        }
    }

    object? IList.this[int index]
    {
        get => _parameters[index];
        set
        {
            ArgumentNullException.ThrowIfNull(value);
            this[index] = (DbParameter)value;
        }
    }

    public bool Contains(string parameterName)
        => _nameMap.ContainsKey(parameterName);

    public int IndexOf(string parameterName)
        => _nameMap.GetValueOrDefault(parameterName, -1);

    public void RemoveAt(string parameterName)
    {
        if (!_nameMap.Remove(parameterName, out int index))
        {
            throw new ArgumentException(
                $"A parameter by the given name does not exist in this collection.  Key: {parameterName}");
        }
        
        _parameters.RemoveAt(index);
        
        // Update indices of parameters that occur after the removed parameter
        foreach (var key in _nameMap.Keys)
        {
            ref int value = ref CollectionsMarshal.GetValueRefOrNullRef(_nameMap, key);
            if (value > index) --value;
        }
    }

    object IDataParameterCollection.this[string parameterName]
    {
        get => this[parameterName];
        set => this[parameterName] = (DbParameter)value;
    }

    /// <summary>
    /// Set (the value of) a parameter keyed by its name.
    /// </summary>
    /// <param name="parameterName">
    /// The name of the parameter to set.
    /// </param>
    public DbParameter this[string parameterName]
    {
        get
        {
            int index = _nameMap[parameterName];
            return _parameters[index];
        }
        set
        {
            if (_nameMap.TryGetValue(parameterName, out int index))
                this[index] = value;
            else
                Add(parameterName, value);
        }
    }
}
