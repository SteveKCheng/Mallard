using System;

namespace Mallard;

/// <summary>
/// Wrapper around a string that is to be treated as a SQL identifier,
/// for quoting it in SQL statements.
/// </summary>
/// <param name="name">The SQL identifier, before quoting. </param>
internal readonly struct SqlIdentifier(string name) : ISpanFormattable
{
    public string Name { get; } = name;
    
    public string ToString(string? format, IFormatProvider? formatProvider)
    {
        var countDoubleQuotes = Name.AsSpan().Count('"');
        var requiredLength = Name.Length + countDoubleQuotes + 2;
        return string.Create(requiredLength, (self: this, format, formatProvider), 
            static (span, state) => {
                state.self.TryFormat(span, out _, state.format, state.formatProvider);
            });
    }
    
    public bool TryFormat(Span<char> destination, out int charsWritten, ReadOnlySpan<char> format,
                          IFormatProvider? provider)
    {
        // Count double-quote characters, as they need to be doubled
        var countDoubleQuotes = Name.AsSpan().Count('"');
        
        if (destination.Length < Name.Length + countDoubleQuotes + 2)
        {
            charsWritten = 0;
            return false;
        }

        int i = 0;
        destination[i++] = '"';
        
        if (countDoubleQuotes > 0)
        {
            foreach (var c in Name)
            {
                destination[i++] = c;
                if (c == '"')
                    destination[i++] = '"';
            }
        }
        else
        {
            Name.AsSpan().CopyTo(destination[i..]);
            i += Name.Length;
        }

        destination[i++] = '"';
        charsWritten = i;
        return true;
    }

    public override string ToString() => ToString(null, null);
}
