using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using Mallard.Types;

namespace Mallard;

public static partial class DuckDbValue
{
    #region Dispatch based on individual type tests
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool TrySetObjectImpl<TReceiver>(TReceiver receiver, object? input) 
        where TReceiver : ISettableDuckDbValue
    {
        if (input is null)
        {
            receiver.SetNull();
            return true;
        }

        if (input is bool b)
        {
            receiver.Set(b);
            return true;
        }

        if (input is sbyte i8)
        {
            receiver.Set(i8);
            return true;
        }

        if (input is byte u8)
        {
            receiver.Set(u8);
            return true;
        }

        if (input is short i16)
        {
            receiver.Set(i16);
            return true;
        }

        if (input is ushort u16)
        {
            receiver.Set(u16);
            return true;
        }

        if (input is int i32)
        {
            receiver.Set(i32);
            return true;
        }

        if (input is uint u32)
        {
            receiver.Set(u32);
            return true;
        }

        if (input is long i64)
        {
            receiver.Set(i64);
            return true;
        }

        if (input is ulong u64)
        {
            receiver.Set(u64);
            return true;
        }

        if (input is Int128 i128)
        {
            receiver.Set(i128);
            return true;
        }

        if (input is UInt128 u128)
        {
            receiver.Set(u128);
            return true;
        }

        if (input is float f32)
        {
            receiver.Set(f32);
            return true;
        }

        if (input is double f64)
        {
            receiver.Set(f64);
            return true;
        }

        if (input is DuckDbDecimal dec2)
        {
            receiver.Set(dec2);
            return true;
        }

        if (input is Decimal dec)
        {
            receiver.Set(DuckDbDecimal.FromDecimal(dec));
            return true;
        }

        if (input is DuckDbDate date2)
        {
            receiver.Set(date2);
            return true;
        }

        if (input is DuckDbTimestamp timestamp)
        {
            receiver.Set(timestamp);
            return true;
        }

        if (input is DateTime dateTime)
        {
            receiver.Set(DuckDbTimestamp.FromDateTime(dateTime));
            return true;
        }

        if (input is DuckDbInterval interval)
        {
            receiver.Set(interval);
            return true;
        }

        if (input is BigInteger bigInteger)
        {
            receiver.Set(bigInteger);
            return true;
        }

        if (input is string s)
        {
            receiver.Set(s);
            return true;
        }

        if (input is byte[] blob)
        {
            receiver.SetBlob(blob);
            return true;
        }

        return false;
    }
    
    #endregion

    #region Public methods to set values as objects or generic types
    
    /// <summary>
    /// Attempt to set the value of a DuckDB parameter from a (boxed) .NET object.   
    /// </summary>
    /// <param name="receiver">The parameter or other object from DuckDB that can accept a value. </param>
    /// <param name="input">The object containing the value to set. </param>
    /// <typeparam name="TReceiver">
    /// The type of <paramref name="receiver" />, explicitly parameterized
    /// to avoid unnecessary boxing when it is value type.
    /// </typeparam>
    /// <returns>
    /// True if the value has been set successfully.  False if the run-time type of <paramref name="input" />
    /// is not supported to use as the value of a DuckDB parameter.
    /// </returns>
    public static bool TrySetObject<TReceiver>(this TReceiver receiver, object? input) 
        where TReceiver : ISettableDuckDbValue
        => TrySetObjectImpl(receiver, input);

    /// <summary>
    /// Set the value of a DuckDB parameter from a (boxed) .NET object.   
    /// </summary>
    /// <param name="receiver">The parameter or other object from DuckDB that can accept a value. </param>
    /// <param name="input">The object containing the value to set. </param>
    /// <typeparam name="TReceiver">
    /// The type of <paramref name="receiver" />, explicitly parameterized
    /// to avoid unnecessary boxing when it is value type.
    /// </typeparam>
    /// <exception cref="NotSupportedException">
    /// The run-time type of <paramref name="input" /> cannot be converted to a DuckDB value.
    /// </exception>
    public static void SetObject<TReceiver>(this TReceiver receiver, object? input)
        where TReceiver : ISettableDuckDbValue
    {
        if (!receiver.TrySetObject(input))
        {
            throw new NotSupportedException(
                $"Cannot set object of type {input!.GetType().Name} into a DuckDB parameter. ");
        }
    }

    /// <summary>
    /// Attempt to set the value of a DuckDB parameter from a value of generic type.   
    /// </summary>
    /// <param name="receiver">The parameter or other object from DuckDB that can accept a value. </param>
    /// <param name="input">The value to set. </param>
    /// <typeparam name="TReceiver">
    /// The type of <paramref name="receiver" />, explicitly parameterized
    /// to avoid unnecessary boxing when it is value type.
    /// </typeparam>
    /// <typeparam name="TInput">
    /// The .NET type of the value to set.
    /// </typeparam>
    /// <returns>
    /// True if the value has been set successfully.  False if the run-time type of <paramref name="input" />
    /// is not supported to use as the value of a DuckDB parameter.
    /// </returns>
    public static bool TrySetGeneric<TReceiver, TInput>(this TReceiver receiver, TInput input)
        where TReceiver : ISettableDuckDbValue
        => TrySetObjectImpl(receiver, (object?)input);

    /// <summary>
    /// Set the value of a DuckDB parameter from a value of generic type.   
    /// </summary>
    /// <param name="receiver">The parameter or other object from DuckDB that can accept a value. </param>
    /// <param name="input">The value to set. </param>
    /// <typeparam name="TReceiver">
    /// The type of <paramref name="receiver" />, explicitly parameterized
    /// to avoid unnecessary boxing when it is value type.
    /// </typeparam>
    /// <typeparam name="TInput">
    /// The .NET type of the value to set.
    /// </typeparam>
    /// <exception cref="NotSupportedException">
    /// The run-time type of <paramref name="input" /> cannot be converted to a DuckDB value.
    /// </exception>
    public static void SetGeneric<TReceiver, TInput>(this TReceiver receiver, TInput input)
        where TReceiver : ISettableDuckDbValue
    {
        if (!receiver.TrySetGeneric(input))
        {
            throw new NotSupportedException(
                $"Cannot set object of type {typeof(TInput).Name} into a DuckDB parameter. ");
        }
    }
    
    #endregion
}
