using System;
using System.Runtime.InteropServices;
using System.Runtime.InteropServices.Marshalling;
using System.Text;

namespace DuckDB.C_API;

[CustomMarshaller(managedType: typeof(string), 
                  marshalMode: MarshalMode.ManagedToUnmanagedOut, 
                  marshallerType: typeof(Utf8StringMarshallerWithFree))]
internal static unsafe class Utf8StringMarshallerWithFree
{
    public static string ConvertToManaged(byte* p)
    {
        if (p == null)
            return string.Empty;

        try
        {
            var utf8Span = MemoryMarshal.CreateReadOnlySpanFromNullTerminated(p);
            return Encoding.UTF8.GetString(utf8Span);
        }
        finally
        {
            NativeMethods.duckdb_free(p);
        }
    }
}

[CustomMarshaller(managedType: typeof(string),
                  marshalMode: MarshalMode.ManagedToUnmanagedOut,
                  marshallerType: typeof(Utf8StringMarshallerWithoutFree))]
internal static unsafe class Utf8StringMarshallerWithoutFree
{
    public static string ConvertToManaged(byte* p)
    {
        if (p == null)
            return string.Empty;

        var utf8Span = MemoryMarshal.CreateReadOnlySpanFromNullTerminated(p);
        return Encoding.UTF8.GetString(utf8Span);
    }
}
