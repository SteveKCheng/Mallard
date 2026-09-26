using System;
using Mallard.Interop;
using Mallard.Types;

namespace Mallard;

public partial class DuckDbAppender
{
    public readonly unsafe struct ItemState : ISettableDuckDbValue
    {
        private readonly DuckDbAppender _parent;
        private readonly uint _sequenceCounter;

        private void CheckSequenceCounter()
        {
            if (_parent._sequenceCounter != _sequenceCounter)
                throw new InvalidOperationException("Attempt to re-set a value to DuckDbAppender.ItemState. ");
        }
        
        #region Implementation of ISettableDuckDbValue
        
        void ISettableDuckDbValue.SetNativeValue(_duckdb_value* nativeValue)
        {
            using var _ = _parent._barricade.EnterScope(_parent);
            try
            {
                CheckSequenceCounter();
                _parent.ThrowOnAppendFailure(NativeMethods.duckdb_append_value(_parent._nativeObj, nativeValue));
                _parent._sequenceCounter++;
            }
            finally
            {
                NativeMethods.duckdb_destroy_value(ref nativeValue);
            }
        }
        
        void ISettableDuckDbValue.SetNull()
        {
            using var _ = _parent._barricade.EnterScope(_parent);
            CheckSequenceCounter();
            _parent.ThrowOnAppendFailure(NativeMethods.duckdb_append_null(_parent._nativeObj));
            _parent._sequenceCounter++;
        }

        void ISettableDuckDbValue.SetBoolean(bool value)
        {
            using var _ = _parent._barricade.EnterScope(_parent);
            CheckSequenceCounter();
            _parent.ThrowOnAppendFailure(NativeMethods.duckdb_append_bool(_parent._nativeObj, value));
            _parent._sequenceCounter++;
        }

        void ISettableDuckDbValue.SetInt8(sbyte value)
        {
            using var _ = _parent._barricade.EnterScope(_parent);
            CheckSequenceCounter();
            _parent.ThrowOnAppendFailure(NativeMethods.duckdb_append_int8(_parent._nativeObj, value));
            _parent._sequenceCounter++;
        }

        void ISettableDuckDbValue.SetInt16(short value)
        {
            using var _ = _parent._barricade.EnterScope(_parent);
            CheckSequenceCounter();
            _parent.ThrowOnAppendFailure(NativeMethods.duckdb_append_int16(_parent._nativeObj, value));
            _parent._sequenceCounter++;
        }
    
        void ISettableDuckDbValue.SetInt32(int value)
        {
            using var _ = _parent._barricade.EnterScope(_parent);
            CheckSequenceCounter();
            _parent.ThrowOnAppendFailure(NativeMethods.duckdb_append_int32(_parent._nativeObj, value));
            _parent._sequenceCounter++;
        }

        void ISettableDuckDbValue.SetInt64(long value)
        {
            using var _ = _parent._barricade.EnterScope(_parent);
            CheckSequenceCounter();
            _parent.ThrowOnAppendFailure(NativeMethods.duckdb_append_int64(_parent._nativeObj, value));
            _parent._sequenceCounter++;
        }

        void ISettableDuckDbValue.SetInt128(Int128 value)
        {
            using var _ = _parent._barricade.EnterScope(_parent);
            CheckSequenceCounter();
            _parent.ThrowOnAppendFailure(NativeMethods.duckdb_append_hugeint(_parent._nativeObj, value));
            _parent._sequenceCounter++;
        }

        void ISettableDuckDbValue.SetUInt8(byte value)
        {
            using var _ = _parent._barricade.EnterScope(_parent);
            CheckSequenceCounter();
            _parent.ThrowOnAppendFailure(NativeMethods.duckdb_append_uint8(_parent._nativeObj, value));
            _parent._sequenceCounter++;
        }

        void ISettableDuckDbValue.SetUInt16(ushort value)
        {
            using var _ = _parent._barricade.EnterScope(_parent);
            CheckSequenceCounter();
            _parent.ThrowOnAppendFailure(NativeMethods.duckdb_append_uint16(_parent._nativeObj, value));
            _parent._sequenceCounter++;
        }
    
        void ISettableDuckDbValue.SetUInt32(uint value)
        {
            using var _ = _parent._barricade.EnterScope(_parent);
            CheckSequenceCounter();
            _parent.ThrowOnAppendFailure(NativeMethods.duckdb_append_uint32(_parent._nativeObj, value));
            _parent._sequenceCounter++;
        }

        void ISettableDuckDbValue.SetUInt64(ulong value)
        {
            using var _ = _parent._barricade.EnterScope(_parent);
            CheckSequenceCounter();
            _parent.ThrowOnAppendFailure(NativeMethods.duckdb_append_uint64(_parent._nativeObj, value));
            _parent._sequenceCounter++;
        }

        void ISettableDuckDbValue.SetUInt128(UInt128 value)
        {
            using var _ = _parent._barricade.EnterScope(_parent);
            CheckSequenceCounter();
            _parent.ThrowOnAppendFailure(NativeMethods.duckdb_append_uhugeint(_parent._nativeObj, value));
            _parent._sequenceCounter++;
        }

        void ISettableDuckDbValue.SetFloat(float value)
        {
            using var _ = _parent._barricade.EnterScope(_parent);
            CheckSequenceCounter();
            _parent.ThrowOnAppendFailure(NativeMethods.duckdb_append_float(_parent._nativeObj, value));
            _parent._sequenceCounter++;
        }

        void ISettableDuckDbValue.SetDouble(double value)
        {
            using var _ = _parent._barricade.EnterScope(_parent);
            CheckSequenceCounter();
            _parent.ThrowOnAppendFailure(NativeMethods.duckdb_append_double(_parent._nativeObj, value));
            _parent._sequenceCounter++;
        }

        // N.B. There is no duckdb_append_decimal so fall back to default implementation
        //      that calls duckdb_create_decimal followed by duckdb_append_value. 

        void ISettableDuckDbValue.SetDate(DuckDbDate value)
        {
            using var _ = _parent._barricade.EnterScope(_parent);
            CheckSequenceCounter();
            _parent.ThrowOnAppendFailure(NativeMethods.duckdb_append_date(_parent._nativeObj, value));
            _parent._sequenceCounter++;
        }

        void ISettableDuckDbValue.SetTimestamp(DuckDbTimestamp value)
        {
            using var _ = _parent._barricade.EnterScope(_parent);
            CheckSequenceCounter();
            _parent.ThrowOnAppendFailure(NativeMethods.duckdb_append_timestamp(_parent._nativeObj, value));
            _parent._sequenceCounter++;
        }

        void ISettableDuckDbValue.SetInterval(DuckDbInterval value)
        {
            using var _ = _parent._barricade.EnterScope(_parent);
            CheckSequenceCounter();
            _parent.ThrowOnAppendFailure(NativeMethods.duckdb_append_interval(_parent._nativeObj, value));
            _parent._sequenceCounter++;
        }

        void ISettableDuckDbValue.SetStringUtf8(byte* data, long length)
        {
            using var _ = _parent._barricade.EnterScope(_parent);
            CheckSequenceCounter();
            _parent.ThrowOnAppendFailure(NativeMethods.duckdb_append_varchar_length(_parent._nativeObj, data, length));
            _parent._sequenceCounter++;
        }

        void ISettableDuckDbValue.SetBlob(byte* data, long length)
        {
            using var _ = _parent._barricade.EnterScope(_parent);
            CheckSequenceCounter();
            _parent.ThrowOnAppendFailure(NativeMethods.duckdb_append_blob(_parent._nativeObj, data, length));
            _parent._sequenceCounter++;
        }
        
        #endregion

        internal ItemState(DuckDbAppender parent)
        {
            _parent = parent;
            
            // N.B. The counter is read without taking a lock.  That's fine because the value is verified
            //      in one of the Set* methods under a lock.
            _sequenceCounter = parent._sequenceCounter;
        }
    }
    
}