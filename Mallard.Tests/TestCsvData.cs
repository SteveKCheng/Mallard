using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Linq;
using CsvHelper;
using CsvHelper.Configuration;
using CsvHelper.TypeConversion;

namespace Mallard.Tests;

// Wrapper around ImmutableArray to implement structural equality.
internal readonly struct ValueArray<T>(ImmutableArray<T> values) 
    : IReadOnlyList<T>, IEquatable<ValueArray<T>> where T : IEquatable<T>
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
}

internal static class ImmutableArrayExtensions
{
    public static ValueArray<T>? ToValueArray<T>(this ImmutableArray<T>? values) where T : IEquatable<T> 
        => values.HasValue ? new ValueArray<T>(values.GetValueOrDefault()) : null;
    public static ValueArray<T> ToValueArray<T>(this ImmutableArray<T> values) where T : IEquatable<T>
        => new(values);
}

// Turn a string from the CSV file into a list of strings, assuming the item separator is ';'
internal sealed class StringListConverter : DefaultTypeConverter
{
    public override object? ConvertFromString(string? text, IReaderRow row, MemberMapData memberMapData)
        => text != null ? ImmutableArray.Create(text.Split(';')).ToValueArray() : null;
}

// Enum defined consistently with Recipes.sql
internal enum 菜類_enum
{
    豬, 牛, 羊, 雞, 鴨鴿鵝, 蔬菜, 蛋, 豆腐, 魚, 蝦, 蟹, 貝類
}

// One row of the Recipes.csv in strongly-typed format
internal sealed record Recipe
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
        using var textReader = File.OpenText(Path.Combine(Program.TestDataDirectory, "Recipes.csv"));
        using var csv = new CsvReader(textReader, new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            HasHeaderRecord = true,
        });
        csv.Context.TypeConverterCache.AddConverter<ValueArray<string>>(new StringListConverter());
        csv.Context.TypeConverterOptionsCache.GetOptions<ValueArray<string>>().NullValues.Add("");
        
        return csv.GetRecords<Recipe>().OrderBy(r => r.頁).ToList();
    }

    private static ValueArray<string>? ReadList(in DuckDbVectorRawReader<DuckDbListRef> vector, int index)
    {
        if (!vector.TryGetItem(index, out var listRef))
            return null;

        var childrenVector = vector.GetChildrenVector<string>();
        var arrayBuilder = ImmutableArray.CreateBuilder<string>(listRef.Length);
        for (int i = 0; i < listRef.Length; ++i)
            arrayBuilder.Add(childrenVector.GetItem(listRef.Offset + i));
        return arrayBuilder.DrainToImmutable().ToValueArray();
    }

    [Fact]
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
            static long IntPow10(int n)
            {
                ArgumentOutOfRangeException.ThrowIfLessThan(n, 0);
                long v = 1;
                while (n-- > 0) v = checked(v * 10);
                return v;
            }

            hasChunk = dbResult.ProcessNextChunk(false, (in DuckDbChunkReader reader, bool _) =>
            {
                var 頁column = reader.GetColumn<int>(0);
                var 菜類column = reader.GetColumn<byte>(1);
                var 菜式column = reader.GetColumn<string>(2);
                var 份量對應人數column = reader.GetColumnRaw<short>(3);
                var 材料column = reader.GetColumnRaw<DuckDbListRef>(4);
                var 醃料column = reader.GetColumnRaw<DuckDbListRef>(5);
                var 調味column = reader.GetColumnRaw<DuckDbListRef>(6);
                var 芡汁column = reader.GetColumnRaw<DuckDbListRef>(7);

                long divisor = IntPow10(份量對應人數column.DecimalScale);

                for (int i = 0; i < reader.Length; ++i)
                {
                    recipesDb.Add(new Recipe
                    {
                        頁 = 頁column.GetItem(i),
                        菜類 = (菜類_enum)菜類column.GetItem(i),
                        菜式 = 菜式column.GetItem(i),
                        份量對應人數 = new decimal(份量對應人數column.GetItem(i)) / divisor,
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
    [Fact]
    public void ReadListVector2()
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
                var 菜類column = reader.GetColumn<byte>(1);
                var 菜式column = reader.GetColumn<string>(2);
                var 份量對應人數column = reader.GetColumn<short>(3);
                var 材料column = reader.GetColumn<ImmutableArray<string>>(4);
                var 醃料column = reader.GetColumn<ImmutableArray<string>>(5);
                var 調味column = reader.GetColumn<ImmutableArray<string>>(6);
                var 芡汁column = reader.GetColumn<ImmutableArray<string>>(7);

                for (int i = 0; i < reader.Length; ++i)
                {
                    recipesDb.Add(new Recipe
                    {
                        頁 = 頁column.GetItem(i),
                        菜類 = (菜類_enum)菜類column.GetItem(i),
                        菜式 = 菜式column.GetItem(i),
                        份量對應人數 = new decimal(份量對應人數column.GetItem(i)) / 100,
                        材料 = 材料column.GetNullableValue(i).ToValueArray(),
                        醃料 = 醃料column.GetNullableValue(i).ToValueArray(),
                        調味 = 調味column.GetNullableValue(i).ToValueArray(),
                        芡汁 = 芡汁column.GetNullableValue(i).ToValueArray(),
                    });
                }

                return false;
            }, out _);
        } while (hasChunk);

        Assert.Equal(recipesCsv, recipesDb);
    }
}
