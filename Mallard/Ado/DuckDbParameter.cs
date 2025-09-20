using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace Mallard.Ado;

internal sealed class DuckDbParameter : DbParameter
{
    public override void ResetDbType() => DbType = DbType.Object;

    public override DbType DbType { get; set; } = DbType.Object;

    public override bool IsNullable { get; set; }

    public override int Size { get; set; }

    public override object? Value { get; set; }

    public override ParameterDirection Direction
    {
        get => throw new System.NotImplementedException();
        set
        {
            if (value != ParameterDirection.Input)
                throw new NotSupportedException("DuckDB supports only parameters in the input direction. ");
        }
    }
    
    [AllowNull]
    public override string ParameterName
    {
        get => field;
        set => field = value ?? string.Empty;
    } = string.Empty;

    [AllowNull]
    public override string SourceColumn
    {
        get => field;
        set => field = value ?? string.Empty;
    } = string.Empty;

    public override bool SourceColumnNullMapping { get; set; }

    public override DataRowVersion SourceVersion { get; set; }
}
