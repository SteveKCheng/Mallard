using System;
using Mallard.Interop;
using Mallard.Types;

namespace Mallard;

public partial class DuckDbStatement
{
    /// <summary>
    /// A formal parameter in a prepared statement from DuckDB.
    /// </summary>
    /// <remarks>
    /// The value of the parameter can be set with any of the methods from
    /// <see cref="DuckDbValue" />.    
    /// </remarks>
    public unsafe readonly struct Parameter : ISettableDuckDbValue
    {
        private readonly DuckDbStatement _parent;
        private readonly int _index;

        internal Parameter(DuckDbStatement parent, int index)
        {
            _parent = parent;
            _index = index;
        }
        
        /// <summary>
        /// The name of this formal parameter within the prepared statement.
        /// </summary>
        /// <value>
        /// The name of the parameter as a string.
        /// For positional parameters in the SQL statement, the name is the decimal
        /// representation of the ordinal (in ASCII digits, no leading zeros).
        /// Note the returned string is not cached.
        /// </value>
        /// <exception cref="ObjectDisposedException">
        /// The containing prepared statement of this parameter has already been
        /// disposed.  (Retrieving the name requires querying the native
        /// prepared statement object from DuckDB.)
        /// </exception>
        public string Name => _parent.GetParameterName(_index);
        
        /// <summary>
        /// The index/position of this formal parameter within the prepared statement.
        /// </summary>
        /// <value>
        /// Index of the parameter, ranging from 1 to the total number of parameters.
        /// </value>
        public int Index => _index;

        #region Implementation of ISettableDuckDbValue
        
        void ISettableDuckDbValue.SetNativeValue(_duckdb_value* nativeValue)
            => _parent.BindParameter(_index, ref nativeValue);

        void ISettableDuckDbValue.SetNull()
        {
            using var _ = _parent._barricade.EnterScope(_parent);
            _parent.ThrowOnBindFailure(NativeMethods.duckdb_bind_null(_parent._nativeStatement, _index));
        }

        void ISettableDuckDbValue.SetBoolean(bool value)
        {
            using var _ = _parent._barricade.EnterScope(_parent);
            _parent.ThrowOnBindFailure(NativeMethods.duckdb_bind_boolean(_parent._nativeStatement, _index, value));
        }

        void ISettableDuckDbValue.SetInt8(sbyte value)
        {
            using var _ = _parent._barricade.EnterScope(_parent);
            _parent.ThrowOnBindFailure(NativeMethods.duckdb_bind_int8(_parent._nativeStatement, _index, value));
        }

        void ISettableDuckDbValue.SetInt16(short value)
        {
            using var _ = _parent._barricade.EnterScope(_parent);
            _parent.ThrowOnBindFailure(NativeMethods.duckdb_bind_int16(_parent._nativeStatement, _index, value));
        }
    
        void ISettableDuckDbValue.SetInt32(int value)
        {
            using var _ = _parent._barricade.EnterScope(_parent);
            _parent.ThrowOnBindFailure(NativeMethods.duckdb_bind_int32(_parent._nativeStatement, _index, value));
        }

        void ISettableDuckDbValue.SetInt64(long value)
        {
            using var _ = _parent._barricade.EnterScope(_parent);
            _parent.ThrowOnBindFailure(NativeMethods.duckdb_bind_int64(_parent._nativeStatement, _index, value));
        }

        void ISettableDuckDbValue.SetInt128(Int128 value)
        {
            using var _ = _parent._barricade.EnterScope(_parent);
            _parent.ThrowOnBindFailure(NativeMethods.duckdb_bind_hugeint(_parent._nativeStatement, _index, value));
        }

        void ISettableDuckDbValue.SetUInt8(byte value)
        {
            using var _ = _parent._barricade.EnterScope(_parent);
            _parent.ThrowOnBindFailure(NativeMethods.duckdb_bind_uint8(_parent._nativeStatement, _index, value));
        }

        void ISettableDuckDbValue.SetUInt16(ushort value)
        {
            using var _ = _parent._barricade.EnterScope(_parent);
            _parent.ThrowOnBindFailure(NativeMethods.duckdb_bind_uint16(_parent._nativeStatement, _index, value));
        }
    
        void ISettableDuckDbValue.SetUInt32(uint value)
        {
            using var _ = _parent._barricade.EnterScope(_parent);
            _parent.ThrowOnBindFailure(NativeMethods.duckdb_bind_uint32(_parent._nativeStatement, _index, value));
        }

        void ISettableDuckDbValue.SetUInt64(ulong value)
        {
            using var _ = _parent._barricade.EnterScope(_parent);
            _parent.ThrowOnBindFailure(NativeMethods.duckdb_bind_uint64(_parent._nativeStatement, _index, value));
        }

        void ISettableDuckDbValue.SetUInt128(UInt128 value)
        {
            using var _ = _parent._barricade.EnterScope(_parent);
            _parent.ThrowOnBindFailure(NativeMethods.duckdb_bind_uhugeint(_parent._nativeStatement, _index, value));
        }

        void ISettableDuckDbValue.SetFloat(float value)
        {
            using var _ = _parent._barricade.EnterScope(_parent);
            _parent.ThrowOnBindFailure(NativeMethods.duckdb_bind_float(_parent._nativeStatement, _index, value));
        }

        void ISettableDuckDbValue.SetDouble(double value)
        {
            using var _ = _parent._barricade.EnterScope(_parent);
            _parent.ThrowOnBindFailure(NativeMethods.duckdb_bind_double(_parent._nativeStatement, _index, value));
        }

        void ISettableDuckDbValue.SetDecimal(DuckDbDecimal value)
        {
            using var _ = _parent._barricade.EnterScope(_parent);
            _parent.ThrowOnBindFailure(NativeMethods.duckdb_bind_decimal(_parent._nativeStatement, _index, value));
        }

        void ISettableDuckDbValue.SetDate(DuckDbDate value)
        {
            using var _ = _parent._barricade.EnterScope(_parent);
            _parent.ThrowOnBindFailure(NativeMethods.duckdb_bind_date(_parent._nativeStatement, _index, value));
        }

        void ISettableDuckDbValue.SetTimestamp(DuckDbTimestamp value)
        {
            using var _ = _parent._barricade.EnterScope(_parent);
            _parent.ThrowOnBindFailure(NativeMethods.duckdb_bind_timestamp(_parent._nativeStatement, _index, value));
        }

        void ISettableDuckDbValue.SetInterval(DuckDbInterval value)
        {
            using var _ = _parent._barricade.EnterScope(_parent);
            _parent.ThrowOnBindFailure(NativeMethods.duckdb_bind_interval(_parent._nativeStatement, _index, value));
        }

        void ISettableDuckDbValue.SetStringUtf8(byte* data, long length)
        {
            using var _ = _parent._barricade.EnterScope(_parent);
            _parent.ThrowOnBindFailure(NativeMethods.duckdb_bind_varchar_length(_parent._nativeStatement, _index, data, length));
        }

        void ISettableDuckDbValue.SetBlob(byte* data, long length)
        {
            using var _ = _parent._barricade.EnterScope(_parent);
            _parent.ThrowOnBindFailure(NativeMethods.duckdb_bind_blob(_parent._nativeStatement, _index, data, length));
        }

        #endregion
        
        #region Type information
        
        /// <summary>
        /// The DuckDB type of this parameter.
        /// </summary>
        /// <value>
        /// The DuckDB kind of the value that this parameter has been set to, or
        /// the kind of value that this parameter accepts if no value has been set yet.
        /// If the type of an unset (unbound) parameter is indeterminate,
        /// <see cref="DuckDbValueKind.Invalid" /> is returned. 
        /// </value>
        /// <exception cref="ObjectDisposedException">
        /// The containing prepared statement of this parameter has already been
        /// disposed.  (This method internally requires querying the native
        /// prepared statement object from DuckDB.)
        /// </exception>
        /// <remarks>
        /// On setting (binding) a value on a parameter, DuckDB will automatically cast
        /// the value to the parameter's type (if not <see cref="DuckDbValueKind.Invalid" />),
        /// so consulting this property is usually not necessary.
        /// </remarks>
        public DuckDbValueKind ValueKind => _parent.GetParameterValueKind(_index);
        
        #endregion
    }
}
