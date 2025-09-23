using System;
using Mallard.Interop;

namespace Mallard;

public partial class DuckDbStatement
{
    /// <summary>
    /// A formal parameter in a prepared statement from DuckDB.
    /// </summary>
    /// <remarks>
    /// The value of the parameter can be set with any of the methods from
    /// <see cref="DuckDbValue" />.    
    /// </remarks>
    public unsafe readonly struct Parameter : ISettableDuckDbValue
    {
        private readonly DuckDbStatement _parent;
        private readonly int _index;

        void ISettableDuckDbValue.SetNativeValue(_duckdb_value* nativeValue)
            => _parent.BindParameter(_index, ref nativeValue);

        internal Parameter(DuckDbStatement parent, int index)
        {
            _parent = parent;
            _index = index;
        }
        
        /// <summary>
        /// The name of this formal parameter within the prepared statement.
        /// </summary>
        /// <value>
        /// The name of the parameter as a string.
        /// For positional parameters in the SQL statement, the name is the decimal
        /// representation of the ordinal (in ASCII digits, no leading zeros).
        /// Note the returned string is not cached.
        /// </value>
        /// <exception cref="ObjectDisposedException">
        /// The containing prepared statement of this parameter has already been
        /// disposed.  (Retrieving the name requires querying the native
        /// prepared statement object from DuckDB.)
        /// </exception>
        public string Name => _parent.GetParameterName(_index);
        
        /// <summary>
        /// The DuckDB type of this parameter.
        /// </summary>
        /// <value>
        /// The DuckDB type of value that this parameter accepts. 
        /// </value>
        /// <exception cref="ObjectDisposedException">
        /// The containing prepared statement of this parameter has already been
        /// disposed.  (This method internally requires querying the native
        /// prepared statement object from DuckDB.)
        /// </exception>
        public DuckDbValueKind ValueKind => _parent.GetParameterValueKind(_index);

        /// <summary>
        /// The index/position of this formal parameter within the prepared statement.
        /// </summary>
        /// <value>
        /// Index of the parameter, ranging from 1 to the total number of parameters.
        /// </value>
        public int Index => _index;
    }
}
