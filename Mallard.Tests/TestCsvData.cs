using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using Dameng.SepEx;
using nietras.SeparatedValues;
using Xunit;
using TUnit.Core;

namespace Mallard.Tests;

// Wrapper around ImmutableArray to implement structural equality.
// Also handles parsing from CSV as a semicolon-delimited list since
// Dameng.SepEx does not support non-intrusive type converters.
internal readonly struct ValueArray<T>(ImmutableArray<T> values) 
    : IReadOnlyList<T>, IEquatable<ValueArray<T>>, ISpanParsable<ValueArray<T>>
    where T : IEquatable<T>, ISpanParsable<T>
{
    private readonly ImmutableArray<T> _values = values;
    public T this[int index] => _values[index];
    public int Count => _values.Length;
    public IEnumerator<T> GetEnumerator() => ((IEnumerable<T>)_values).GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable)_values).GetEnumerator();

    public override int GetHashCode()
    {
        int hash = HashCode.Combine(_values.Length);
        for (int i = 0; i < _values.Length; ++i)
            hash = HashCode.Combine(hash, _values[i]);
        return hash;
    }

    public override bool Equals([NotNullWhen(true)] object? obj)
        => obj is ValueArray<T> other && this.Equals(other);

    public bool Equals(ValueArray<T> other)
        => _values.SequenceEqual(other._values);

    static ValueArray<T> IParsable<ValueArray<T>>.Parse(string s, IFormatProvider? provider)
        => Parse(s.AsSpan(), provider); 
    
    static bool IParsable<ValueArray<T>>.TryParse([NotNullWhen(true)] string? s, IFormatProvider? provider, out ValueArray<T> result)
        => TryParse(s.AsSpan(), provider, out result);

    public static ValueArray<T> Parse(ReadOnlySpan<char> s, IFormatProvider? provider)
    {
        if (TryParse(s, provider, out var result))
            return result;
        
        throw new FormatException($"Could not parse semicolon-delimited list of type {typeof(T).Name}");
    }

    public static bool TryParse(ReadOnlySpan<char> s, IFormatProvider? provider, out ValueArray<T> result)
    {
        result = new ValueArray<T>(ImmutableArray<T>.Empty);

        if (s.IsEmpty)
            return true;
        
        // Count number of items to allocate array exactly
        int count = 1;
        foreach (var c in s)
            if (c == ';') ++count;
        
        var builder = ImmutableArray.CreateBuilder<T>(count);
        int i = -1;
        do
        {
            s = s[(i + 1)..];
            i = s.IndexOf(';');
            var t = (i >= 0) ? s[..i] : s;
            if (!T.TryParse(t, provider, out var item))
                return false;
            builder.Add(item);
        } while (i >= 0);

        result = new ValueArray<T>(builder.MoveToImmutable());
        return true;
    }
}

internal static class ImmutableArrayExtensions
{
    public static ValueArray<T>? ToValueArray<T>(this ImmutableArray<T>? values) 
        where T : IEquatable<T>, ISpanParsable<T> 
        => values.HasValue ? new ValueArray<T>(values.GetValueOrDefault()) : null;
    public static ValueArray<T> ToValueArray<T>(this ImmutableArray<T> values) 
        where T : IEquatable<T>, ISpanParsable<T>
        => new(values);
}

// Enum defined consistently with Recipes.sql
internal enum 菜類_enum
{
    豬, 牛, 羊, 雞, 鴨鴿鵝, 蔬菜, 蛋, 豆腐, 魚, 蝦, 蟹, 貝類
}

// One row of the Recipes.csv in strongly-typed format
[GenSepParsable]
internal sealed partial record Recipe
{
    public int 頁 { get; init; }
    public 菜類_enum 菜類 { get; init; }
    public string? 菜式 { get; init; }
    public decimal 份量對應人數 { get; init; }
    public ValueArray<string>? 材料 { get; init; }
    public ValueArray<string>? 醃料 { get; init; }
    public ValueArray<string>? 調味 { get; init; }
    public ValueArray<string>? 芡汁 { get; init; }
}

public class TestCsvData
{
    private static List<Recipe> GetRecipes()
    {
        var csvOptions = new SepReaderOptions()
        {
            HasHeader = true,
            Unescape = true,
        };

        using var csvReader = csvOptions.FromFile(Path.Combine(Program.TestDataDirectory, "Recipes.csv"));
        return csvReader.GetRecords<Recipe>().OrderBy(r => r.頁).ToList();
    }

    private static ValueArray<string>? ReadList(in DuckDbVectorRawReader<DuckDbListRef> vector, int index)
    {
        if (!vector.TryGetItem(index, out var listRef))
            return null;

        var childrenVector = vector.GetChildrenRaw<DuckDbString>();
        var arrayBuilder = ImmutableArray.CreateBuilder<string>(listRef.Length);
        for (int i = 0; i < listRef.Length; ++i)
            arrayBuilder.Add(childrenVector.GetItem(listRef.Offset + i).ToString());
        return arrayBuilder.DrainToImmutable().ToValueArray();
    }

    [Test]
    public void ReadListVector1()
    {
        var recipesCsv = GetRecipes();
        using var dbConn = new DuckDbConnection("");

        // Execute SQL statement in file to parse CSV and create a table from the data
        var sql = File.ReadAllText(Path.Combine(Program.TestDataDirectory, "Recipes.sql"));
        sql = sql.Replace("'Recipes.csv'", $"'{Path.Combine(Program.TestDataDirectory, "Recipes.csv")}'");
        dbConn.ExecuteNonQuery(sql);

        using var dbResult = dbConn.Execute(@"SELECT * FROM 家常小菜 ORDER BY 頁 ASC");

        var recipesDb = new List<Recipe>();

        bool hasChunk;
        do
        {
            hasChunk = dbResult.ProcessNextChunk(false, (in DuckDbChunkReader reader, bool _) =>
            {
                var 頁column = reader.GetColumn<int>(0);
                var 菜類column = reader.GetColumnRaw<byte>(1);
                var 菜式column = reader.GetColumn<string>(2);
                var 份量對應人數column = reader.GetColumnRaw<short>(3);
                var 材料column = reader.GetColumnRaw<DuckDbListRef>(4);
                var 醃料column = reader.GetColumnRaw<DuckDbListRef>(5);
                var 調味column = reader.GetColumnRaw<DuckDbListRef>(6);
                var 芡汁column = reader.GetColumnRaw<DuckDbListRef>(7);

                for (int i = 0; i < reader.Length; ++i)
                {
                    recipesDb.Add(new Recipe
                    {
                        頁 = 頁column.GetItem(i),
                        菜類 = (菜類_enum)菜類column.GetItem(i),
                        菜式 = 菜式column.GetItem(i),
                        份量對應人數 = DuckDbDecimal.ConvertToDecimal(份量對應人數column.GetItem(i), 
                                                                    份量對應人數column.ColumnInfo.DecimalScale),
                        材料 = ReadList(材料column, i),
                        醃料 = ReadList(醃料column, i),
                        調味 = ReadList(調味column, i),
                        芡汁 = ReadList(芡汁column, i)
                    });
                }

                return false;
            }, out _);
        } while(hasChunk);

        Assert.Equal(recipesCsv, recipesDb);
    }

    // Same as ReadListVector1 but without any "raw" conversions
    [Test]
    public void ReadListVector2()
    {
        var recipesCsv = GetRecipes();
        using var dbConn = new DuckDbConnection("");

        // Execute SQL statement in file to parse CSV and create a table from the data
        var sql = File.ReadAllText(Path.Combine(Program.TestDataDirectory, "Recipes.sql"));
        sql = sql.Replace("'Recipes.csv'", $"'{Path.Combine(Program.TestDataDirectory, "Recipes.csv")}'");
        dbConn.ExecuteNonQuery(sql);

        using var dbResult = dbConn.Execute(@"SELECT * FROM 家常小菜 ORDER BY 頁 ASC");

        // Check column names.  3 times to cover the internal caching.
        for (int i = 0; i < 3; ++i)
        {
            Assert.Equal("頁", dbResult.GetColumnName(0));
            Assert.Equal("菜類", dbResult.GetColumnName(1));
            Assert.Equal("菜式", dbResult.GetColumnName(2));
            Assert.Equal("份量對應人數", dbResult.GetColumnName(3));
            Assert.Equal("材料", dbResult.GetColumnName(4));
            Assert.Equal("醃料", dbResult.GetColumnName(5));
            Assert.Equal("調味", dbResult.GetColumnName(6));
            Assert.Equal("芡汁", dbResult.GetColumnName(7));
        }

        var recipesDb = new List<Recipe>();

        int totalRows = dbResult.ProcessAllChunks(false, (in DuckDbChunkReader reader, bool _) =>
        {
            var 頁column = reader.GetColumn<int>(0);
            var 菜類column = reader.GetColumn<菜類_enum>(1);
            var 菜式column = reader.GetColumn<string>(2);
            var 份量對應人數column = reader.GetColumn<Decimal>(3);
            var 材料column = reader.GetColumn<ImmutableArray<string>>(4);
            var 醃料column = reader.GetColumn<ImmutableArray<string>>(5);
            var 調味column = reader.GetColumn<ImmutableArray<string>>(6);
            var 芡汁column = reader.GetColumn<ImmutableArray<string>>(7);

            var 頁columnBoxed = reader.GetColumn<object>(0);
            var 菜式columnBoxed = reader.GetColumn<object>(2);
            var 份量對應人數columnBoxed = reader.GetColumn<object>(3);
            var 材料columnBoxed = reader.GetColumn<object>(4);

            for (int i = 0; i < reader.Length; ++i)
            {
                var 頁i = 頁column.GetItem(i);
                var 菜式i = 菜式column.GetItem(i);
                var 份量對應人數i = 份量對應人數column.GetItem(i);
                var 材料i = 材料column.GetNullableValue(i);

                // Check boxed items are equal to unboxed items
                Assert.Equal((object)頁i, 頁columnBoxed.GetItem(i));
                Assert.Equal((object)菜式i, 菜式columnBoxed.GetItem(i));
                Assert.Equal((object)份量對應人數i, 份量對應人數columnBoxed.GetItem(i));
                Assert.Equal((IEnumerable<string>?)材料i, (IEnumerable<string>)材料columnBoxed.GetItem(i));

                recipesDb.Add(new Recipe
                {
                    頁 = 頁i,
                    菜類 = 菜類column.GetItem(i),
                    菜式 = 菜式i,
                    份量對應人數 = 份量對應人數i,
                    材料 = 材料i.ToValueArray(),
                    醃料 = 醃料column.GetNullableValue(i).ToValueArray(),
                    調味 = 調味column.GetNullableValue(i).ToValueArray(),
                    芡汁 = 芡汁column.GetNullableValue(i).ToValueArray(),
                });
            }

            return reader.Length;
        }, seed: 0, accumulate: static (n, m) => n + m);

        Assert.Equal(recipesCsv.Count, totalRows);
        Assert.Equal(recipesCsv, recipesDb);
    }
}
