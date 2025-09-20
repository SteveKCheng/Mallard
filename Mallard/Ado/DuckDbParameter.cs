using System;
using System.Data;
using System.Diagnostics.CodeAnalysis;

namespace Mallard.Ado;

internal sealed class DuckDbParameter : IDbDataParameter
{
    public DbType DbType { get; set; }

    public ParameterDirection Direction
    {
        get => throw new System.NotImplementedException();
        set
        {
            if (value != ParameterDirection.Input)
                throw new NotSupportedException("DuckDB supports only parameters in the input direction. ");
        }
    }
    
    public bool IsNullable => true;

    [AllowNull]
    public string ParameterName
    {
        get => field;
        set => field = value ?? string.Empty;
    } = string.Empty;

    public object? Value { get; set; }

    [AllowNull]
    public string SourceColumn
    {
        get => field;
        set => field = value ?? string.Empty;
    } = string.Empty;
    
    public DataRowVersion SourceVersion { get; set; }
    
    public byte Precision { get; set; }
    public byte Scale { get; set; }
    public int Size { get; set; }
}
