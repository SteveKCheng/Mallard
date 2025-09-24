using System;
using Mallard.Interop;
using Mallard.Types;

namespace Mallard;

public static unsafe partial class DuckDbValue
{
    /// <summary>
    /// Local context for setting a structure-typed (STRUCT) value into a DuckDB parameter. 
    /// </summary>
    /// <remarks>
    /// Default-initialized instances should not be constructed.
    /// Valid instances can only come as an argument passed by <see cref="SetStruct" />
    /// into its caller-supplied action.  Attempting to use a default-initialized
    /// instance will throw exceptions. 
    /// </remarks>
    public readonly ref struct Struct
    {
        /// <summary>
        /// Array of slots for the native objects for each of the members
        /// of the STRUCT.
        /// </summary>
        /// <remarks>
        /// We would have preferred to use spans instead of raw pointers,
        /// but unfortunately .NET generics do not allow the type parameter
        /// to be a pointer type.
        /// </remarks>
        private readonly _duckdb_value** _memberValues;
        
        /// <summary>
        /// The number of members in the STRUCT.
        /// </summary>
        private readonly int _membersCount; 

        internal Struct(_duckdb_value** memberValues, int membersCount)
        {
            _memberValues = memberValues;
            _membersCount = membersCount;
        }

        /// <summary>
        /// Retrieve the slot to set the value of a member in the STRUCT value.
        /// </summary>
        /// <param name="index">
        /// The index of the member within the STRUCT, numbered from 0 (inclusive)
        /// to the count of all members (exclusive).
        /// </param>
        /// <exception cref="ArgumentOutOfRangeException">
        /// The index is outside the designated range for STRUCT members.
        /// </exception>
        public StructMember this[int index]
        {
            get
            {
                ArgumentOutOfRangeException.ThrowIfNegative(index);
                ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(index, _membersCount);
                return new StructMember(ref _memberValues[index]);
            }
        }

        internal _duckdb_value* MakeNativeValue(_duckdb_logical_type* nativeStructType)
        {
            for (int i = 0; i < _membersCount; ++i)
            {
                if (_memberValues[i] == null)
                {
                    throw new InvalidOperationException(
                        $"At least one of the member values in the STRUCT-type value to input into DuckDB has not been set. Index: {i}.");
                }
            }
            
            return NativeMethods.duckdb_create_struct_value(nativeStructType, _memberValues);
        }
        
        internal void Dispose()
        {
            for (int i = 0; i < _membersCount; ++i)
            {
                if (_memberValues[i] != null)
                {
                    NativeMethods.duckdb_destroy_value(ref _memberValues[i]);
                }
            }
        }
    }

    /// <summary>
    /// Represents a member of struct-typed data that may be set within
    /// the caller-supplied action to <see cref="SetStruct" />.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The extension methods from <see cref="DuckDbValue" /> may be
    /// used freely on an instance of this type.
    /// </para>
    /// <para>
    /// Default-initialized instances should not be constructed.
    /// Attempting to use such instances will result in <see cref="NullReferenceException" />
    /// being thrown.
    /// </para> 
    /// </remarks>
    public readonly ref struct StructMember : ISettableDuckDbValue
    {
        private readonly ref _duckdb_value* _memberValue;

        internal StructMember(ref _duckdb_value* memberValue)
        {
            _memberValue = ref memberValue;
        }

        private void SetNativeValue(_duckdb_value* nativeValue)
        {
            _duckdb_value* valueToDestroy = nativeValue;
            try
            {
                // Read this first.  Forces a NullReferenceException if 
                // this instance is default-initialized.
                var oldMemberValue = _memberValue;

                ThrowOnNullDuckDbValue(nativeValue);

                // Write new value, overwriting the old if any.
                valueToDestroy = oldMemberValue;
                _memberValue = nativeValue;
            }
            finally
            {
                if (valueToDestroy != null)
                    NativeMethods.duckdb_destroy_value(ref valueToDestroy);
            }
        }
        
        #region Implementation of ISettableDuckDbValue
        
        void ISettableDuckDbValue.SetNativeValue(_duckdb_value* nativeValue) => SetNativeValue(nativeValue);
        void ISettableDuckDbValue.SetNull() => SetNativeValue(NativeMethods.duckdb_create_null_value());
        void ISettableDuckDbValue.SetBoolean(bool value) => SetNativeValue(NativeMethods.duckdb_create_bool(value));
        void ISettableDuckDbValue.SetInt8(sbyte value) => SetNativeValue(NativeMethods.duckdb_create_int8(value));
        void ISettableDuckDbValue.SetInt16(short value) => SetNativeValue(NativeMethods.duckdb_create_int16(value));
        void ISettableDuckDbValue.SetInt32(int value) => SetNativeValue(NativeMethods.duckdb_create_int32(value));
        void ISettableDuckDbValue.SetInt64(long value) => SetNativeValue(NativeMethods.duckdb_create_int64(value));
        void ISettableDuckDbValue.SetInt128(Int128 value) => SetNativeValue(NativeMethods.duckdb_create_hugeint(value));
        void ISettableDuckDbValue.SetUInt8(byte value) => SetNativeValue(NativeMethods.duckdb_create_uint8(value));
        void ISettableDuckDbValue.SetUInt16(ushort value) => SetNativeValue(NativeMethods.duckdb_create_uint16(value));
        void ISettableDuckDbValue.SetUInt32(uint value) => SetNativeValue(NativeMethods.duckdb_create_uint32(value));
        void ISettableDuckDbValue.SetUInt64(ulong value) => SetNativeValue(NativeMethods.duckdb_create_uint64(value));
        void ISettableDuckDbValue.SetUInt128(UInt128 value) => SetNativeValue(NativeMethods.duckdb_create_uhugeint(value));
        void ISettableDuckDbValue.SetFloat(float value) => SetNativeValue(NativeMethods.duckdb_create_float(value));
        void ISettableDuckDbValue.SetDouble(double value) => SetNativeValue(NativeMethods.duckdb_create_double(value));
        void ISettableDuckDbValue.SetDecimal(DuckDbDecimal value) => SetNativeValue(NativeMethods.duckdb_create_decimal(value));
        void ISettableDuckDbValue.SetDate(DuckDbDate value) => SetNativeValue(NativeMethods.duckdb_create_date(value));
        void ISettableDuckDbValue.SetTimestamp(DuckDbTimestamp value) => SetNativeValue(NativeMethods.duckdb_create_timestamp(value));
        void ISettableDuckDbValue.SetInterval(DuckDbInterval value) => SetNativeValue(NativeMethods.duckdb_create_interval(value));
        void ISettableDuckDbValue.SetStringUtf8(byte* data, long length) => SetNativeValue(NativeMethods.duckdb_create_varchar_length(data, length));
        void ISettableDuckDbValue.SetBlob(byte* data, long length) => SetNativeValue(NativeMethods.duckdb_create_blob(data, length));
        
        #endregion
    }

    /// <summary>
    /// Set a structural value (STRUCT) into a DuckDB parameter.
    /// </summary>
    /// <param name="receiver">The parameter or other object from DuckDB that can accept a value. </param>
    /// <param name="structType">Describes the STRUCT-typed value to set. </param>
    /// <param name="state">Arbitrary argument passed into <paramref name="action" />. </param>
    /// <param name="action">The code that this method will call to populate the members
    /// of the STRUCT-typed value. </param>
    /// <typeparam name="TReceiver">
    /// The type of <paramref name="receiver" />, explicitly parameterized
    /// to avoid unnecessary boxing when it is value type.
    /// </typeparam>
    /// <typeparam name="TState">
    /// The arbitrary type for the state data/object to pass into <paramref name="action" />.
    /// </typeparam>
    /// <remarks>
    /// <para>
    /// The members of the STRUCT value to set must be passed along to DuckDB together.
    /// Doing that efficiently with one method is difficult especially if the members of
    /// the STRUCT have heterogeneous types.  To retain efficiency without compromising
    /// type/memory safety, this method sets up a temporary local context for buffering
    /// the values of the STRUCT members, and executes a caller-supplied action
    /// (delegate) with that context passed in.  After the caller-supplied action
    /// finishes, the buffered values are set into DuckDB.
    /// </para>
    /// <para>
    /// The local context (which embed pointers to native objects from DuckDB)  
    /// is expressed as ref structs, so they cannot leak outside of the action.
    /// </para>
    /// </remarks>
    /// <exception cref="InvalidOperationException">
    /// One or more members of the structure-typed value has not been set by
    /// <paramref name="action" /> after it returns.
    /// </exception>
    /// <exception cref="ObjectDisposedException">
    /// <paramref name="structType" /> has already been disposed.
    /// </exception>
    public static void SetStruct<TReceiver, TState>(this TReceiver receiver, 
                                                    DuckDbStructColumns structType, 
                                                    in TState state, 
                                                    Action<Struct, TState> action)
        where TReceiver : ISettableDuckDbValue, allows ref struct
        where TState : allows ref struct
        => receiver.SetNativeValue(MakeStructValue(structType, state, action)); 

    private static _duckdb_value* MakeStructValue<TState>(DuckDbStructColumns structType,
                                                          in TState state,
                                                          Action<Struct, TState> action)
        where TState : allows ref struct
    {
        int membersCount = structType.ColumnCount;
        var memberValues = stackalloc _duckdb_value*[membersCount];
        var nativeLogicalType = structType.BorrowNativeLogicalType(out var scope);
        var context = new Struct(memberValues, membersCount);
        
        try
        {
            action(context, state);
            return context.MakeNativeValue(nativeLogicalType);
        }
        finally
        {
            context.Dispose();
            scope.Dispose();
        }
    }
}