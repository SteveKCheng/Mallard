using System;
using System.Runtime.InteropServices;
using System.Runtime.InteropServices.Marshalling;
using System.Text;

namespace DuckDB.C_API;

[CustomMarshaller(managedType: typeof(string), 
                  marshalMode: MarshalMode.ManagedToUnmanagedOut, 
                  marshallerType: typeof(FreeStringMarshaller))]
internal static unsafe class FreeStringMarshaller
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
