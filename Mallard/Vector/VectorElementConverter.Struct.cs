using System;
using System.Collections.Immutable;

namespace Mallard;
using Mallard.Types;

using UnboundStructMember = (DuckDbColumnInfo ColumnInfo, VectorElementConverter UnboundConverter);
using BoundStructMember = (DuckDbVectorInfo Vector, VectorElementConverter Converter);

internal sealed class StructConverter
{
    private sealed class Binder : IConverterBinder<StructConverter>
    {
        private readonly UnboundStructMember[] _members;

        public Binder(DuckDbStructColumns structColumns)
        {
            _members = new UnboundStructMember[structColumns.ColumnCount];

            for (int i = 0; i < _members.Length; ++i)
            {
                _members[i] = (ColumnInfo: structColumns.GetColumnInfo(i),
                               UnboundConverter: structColumns.GetColumnConverter(i, typeof(object)));
            }
        }

        public StructConverter BindToVector(in DuckDbVectorInfo vector)
            => new StructConverter(_members, vector);
    }

    private readonly BoundStructMember[] _members;

    private StructConverter(UnboundStructMember[] unboundMembers, in DuckDbVectorInfo vector)
    {
        _members = new BoundStructMember[unboundMembers.Length];

        for (int i = 0; i < unboundMembers.Length; ++i)
        {
            _members[i] = (Vector: vector.GetStructMemberVectorInfo(i, unboundMembers[i].ColumnInfo),
                           Converter: unboundMembers[i].UnboundConverter.BindToVector(vector));
        }
    }

    private static DuckDbStruct ConvertElement(StructConverter self, in DuckDbVectorInfo _, int index)
    {
        var members = self._members;

        var builder = ImmutableArray.CreateBuilder<object?>(members.Length);
        for (int i = 0; i < members.Length; ++i)
            builder.Add(members[i].Converter.Convert<object>(members[i].Vector, index, false));

        return new DuckDbStruct(builder.MoveToImmutable());
    }

    internal static unsafe VectorElementConverter GetConverter(ref readonly ConverterCreationContext context)
        => VectorElementConverter.Create(new Binder(DuckDbStructColumns.Create(in context)), &ConvertElement);
}
