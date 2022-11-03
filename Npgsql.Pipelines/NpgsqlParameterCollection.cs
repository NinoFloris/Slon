using System.Collections.Immutable;
using System.Runtime.CompilerServices;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace Npgsql.Pipelines;

// TODO we may want to have a few versions internally (maybe auto generated) with say 1, 2, 3, 4, many parameters where NpgsqlParameterCollection is just the wrapping class.
// Could eliminate all parameter/boxing allocations for small queries, and would basically make the lookup path always enabled in the 'many' case, reducing complexity there.

/// <summary>
/// Represents a collection of parameters relevant to a <see cref="NpgsqlCommand"/> as well as their respective mappings to columns in
/// a <see cref="DataSet"/>.
/// </summary>
public sealed class NpgsqlParameterCollection : DbParameterCollection, IEnumerable<KeyValuePair<string, object?>>, IList<NpgsqlDbParameter>, IDataParameterCollection
{
    internal readonly struct ParameterItem
    {
        readonly string _name;
        readonly object? _value;

        ParameterItem(string name, object? value)
        {
            if (value is DbParameter)
            {
                if (value is not NpgsqlDbParameter p)
                    throw new InvalidCastException(
                        $"The DbParameter \"{value}\" is not of type \"{nameof(NpgsqlDbParameter)}\" and cannot be used in this parameter collection, it can be added as a value to an NpgsqlDbParameter if this was intended.");

                if (!name.AsSpan().Equals(CreateNameSpan(p.ParameterName), StringComparison.OrdinalIgnoreCase))
                    throw new ArgumentException("Parameter name must be a case-insensitive match with the property 'ParameterName' on the given NpgsqlParameter.", nameof(name));

                // Prevent any changes from now on as the name may end up being used in the lookup.
                // We don't want the lookup to get out of sync and we also don't want any backreferences from parameter to collection.
                p.NotifyCollectionAdd();
            }

            _name = name;
            _value = value;
        }

        /// The canonical name used for uniqueness.
        public string Name => _name;

        /// Either null, an object or an NpgsqlDbParameter, any other derived DbParameter types are not accepted.
        public object? Value => _value;

        public bool TryGetNpgsqlDbParameter([NotNullWhen(true)]out NpgsqlDbParameter? parameter)
        {
            if (Value is NpgsqlDbParameter p)
            {
                parameter = p;
                return true;
            }

            parameter = default;
            return false;
        }

        public KeyValuePair<string, object?> AsKeyValuePair() => new(_name, _value);

        public bool IsPositional => _name is "";

        static int ComputePrefixLength(string name) => name.Length > 0 && name[0] is '@' or ':' ? 1 : 0;
        public static string CreateName(string parameterName) => parameterName.Substring(ComputePrefixLength(parameterName));
        public static ReadOnlySpan<char> CreateNameSpan(string parameterName) => parameterName.AsSpan(ComputePrefixLength(parameterName));
        public static ParameterItem Create(string name, object? value) => new(CreateName(name), value);
        public static ParameterItem Create(object? value) => new("", value);
    }

    const int LookupThreshold = 5;

    readonly ImmutableArray<ParameterItem>.Builder _parameters = ImmutableArray.CreateBuilder<ParameterItem>(5);

    // Dictionary lookups for GetValue to improve performance.
    Dictionary<string, int>? _caseInsensitiveLookup;

    /// <summary>
    /// Initializes a new instance of the NpgsqlParameterCollection class.
    /// </summary>
    internal NpgsqlParameterCollection() {}

    bool LookupEnabled => _parameters.Count >= LookupThreshold;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    string GetName(int index) => _parameters[index].Name;

    void LookupClear() => _caseInsensitiveLookup?.Clear();
    void LookupAdd(string name, int index) => _caseInsensitiveLookup?.TryAdd(name, index);
    void LookupInsert(string name, int index)
    {
        if (_caseInsensitiveLookup is null)
            return;

        if (!_caseInsensitiveLookup.TryGetValue(name, out var indexCi) || index < indexCi)
        {
            for (var i = index + 1; i < _parameters.Count; i++)
            {
                var parameterName = GetName(i);
                if (_caseInsensitiveLookup.TryGetValue(parameterName, out var currentI) && currentI + 1 == i)
                    _caseInsensitiveLookup[parameterName] = i;
            }

            _caseInsensitiveLookup[name] = index;
        }
    }

    void LookupRemove(string name, int index)
    {
        if (_caseInsensitiveLookup is null)
            return;

        if (_caseInsensitiveLookup.Remove(name))
        {
            for (var i = index; i < _parameters.Count; i++)
            {
                var parameterName = GetName(i);
                if (_caseInsensitiveLookup.TryGetValue(parameterName, out var currentI) && currentI - 1 == i)
                    _caseInsensitiveLookup[parameterName] = i;
            }

            // Fix-up the case-insensitive lookup to point to the next match, if any.
            for (var i = 0; i < _parameters.Count; i++)
            {
                var parameterName = GetName(i);
                if (parameterName.Equals(name, StringComparison.OrdinalIgnoreCase))
                {
                    _caseInsensitiveLookup[name] = i;
                    break;
                }
            }
        }

    }

    void LookupChangeName(ParameterItem item, string oldName, int index)
    {
        if (oldName.Equals(item.Name, StringComparison.OrdinalIgnoreCase))
            return;

        if (oldName.Length != 0)
            LookupRemove(oldName, index);
        if (!item.IsPositional)
            LookupAdd(item.Name, index);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    NpgsqlDbParameter GetOrAddNpgsqlParameter(int index)
    {
        ref var p = ref Unsafe.AsRef(_parameters.ItemRef(index));
        if (p.TryGetNpgsqlDbParameter(out var parameter))
            return parameter;

        return Slow(ref p);

        NpgsqlParameter Slow(ref ParameterItem p)
        {
            var dbParam = new NpgsqlParameter(p.Name, p.Value);
            p = ParameterItem.Create(p.Name, p.Value);
            return dbParam;
        }
    }

    void Replace(int index, string? parameterName, object? value)
    {
        ref var oldValue = ref Unsafe.AsRef(_parameters.ItemRef(index));
        var item = parameterName is null ? ParameterItem.Create(value) : ParameterItem.Create(parameterName, value);
        LookupChangeName(item, oldValue.Name, index);
        oldValue = item;
    }

    public void Add(string parameterName, object? value)
    {
        // Wrap any DbParameter that we don't recognize in our own object.
        if (value is DbParameter and not NpgsqlDbParameter)
            value = NpgsqlDbParameter.Create(parameterName, value);

        var item = ParameterItem.Create(parameterName, value);
        _parameters.Add(item);
        if (!item.IsPositional)
            LookupAdd(item.Name, _parameters.Count - 1);
    }

    public void Add<T>(string parameterName, T? value)
        => Add(parameterName, (object)NpgsqlDbParameter.Create(parameterName, value));

    object? IList.this[int index]
    {
        get => GetParameter(index);
        set => Replace(index, null, value);
    }

    object IDataParameterCollection.this[string parameterName]
    {
        get => GetParameter(parameterName);
        set
        {
            var index = IndexOf(parameterName);
            if (index == -1)
                Add(parameterName, value);
            else
                Replace(index, parameterName, value);
        }
    }

    #region NpgsqlParameterCollection Member

    /// <summary>
    /// Gets the <see cref="NpgsqlParameter"/> with the specified name.
    /// </summary>
    /// <param name="parameterName">The name of the <see cref="NpgsqlParameter"/> to retrieve.</param>
    /// <value>
    /// The <see cref="NpgsqlParameter"/> with the specified name, or a <see langword="null"/> reference if the parameter is not found.
    /// </value>
    public new NpgsqlDbParameter this[string parameterName]
    {
        get
        {
            var index = IndexOf(parameterName);
            if (index == -1)
                throw new ArgumentException("Parameter not found");

            return GetOrAddNpgsqlParameter(index);
        }
        set
        {
            if (value is null)
                throw new ArgumentNullException(nameof(value));

            var index = IndexOf(parameterName);
            if (index == -1)
                Add(parameterName, value);
            else
                Replace(index, parameterName, value);
        }
    }

    /// <summary>
    /// Gets the <see cref="NpgsqlParameter"/> at the specified index.
    /// </summary>
    /// <param name="index">The zero-based index of the <see cref="NpgsqlParameter"/> to retrieve.</param>
    /// <value>The <see cref="NpgsqlParameter"/> at the specified index.</value>
    public new NpgsqlDbParameter this[int index]
    {
        get => GetOrAddNpgsqlParameter(index);
        set
        {
            if (value is null)
                throw new ArgumentNullException(nameof(value));

            Replace(index, value.ParameterName, value);
        }
    }

    /// <summary>
    /// Adds the specified <see cref="NpgsqlParameter"/> object to the <see cref="NpgsqlParameterCollection"/>.
    /// </summary>
    /// <param name="value">The <see cref="NpgsqlParameter"/> to add to the collection.</param>
    /// <returns>The new <see cref="NpgsqlParameter"/> object.</returns>
    public NpgsqlDbParameter Add(NpgsqlDbParameter value)
    {
        Add(value.ParameterName, value);
        return value;
    }

    /// <inheritdoc />
    void ICollection<NpgsqlDbParameter>.Add(NpgsqlDbParameter item)
        => Add(item);

    #endregion

    #region IDataParameterCollection Member

    /// <inheritdoc />
    // ReSharper disable once ImplicitNotNullOverridesUnknownExternalMember
    public override void RemoveAt(string parameterName)
        => RemoveAt(IndexOf(parameterName ?? throw new ArgumentNullException(nameof(parameterName))));

    /// <inheritdoc />
    public override bool Contains(string parameterName)
        => IndexOf(parameterName ?? throw new ArgumentNullException(nameof(parameterName))) != -1;

    /// <inheritdoc />
    public override int IndexOf(string parameterName)
    {
        if (parameterName is null)
            throw new ArgumentNullException(nameof(parameterName));

        var name = ParameterItem.CreateNameSpan(parameterName);

        // Using a dictionary is always faster after around 10 items when matched against reference equality.
        // For string equality this is the case after ~3 items so we take a decent compromise going with 5.
        if (LookupEnabled && name.Length != 0)
        {
            if (_caseInsensitiveLookup is null)
                BuildLookup();

            if (_caseInsensitiveLookup!.TryGetValue(name.ToString(), out var indexCi))
                return indexCi;

            return -1;
        }

        // Do case-insensitive search.
        for (var i = 0; i < _parameters.Count; i++)
        {
            var otherName = GetName(i);
            if (name.Equals(otherName.AsSpan(), StringComparison.OrdinalIgnoreCase))
                return i;
        }

        return -1;

        void BuildLookup()
        {
            _caseInsensitiveLookup = new(_parameters.Count, StringComparer.OrdinalIgnoreCase);

            for (var i = 0; i < _parameters.Count; i++)
            {
                var item = _parameters[i];
                if (!item.IsPositional)
                    LookupAdd(item.Name, i);
            }
        }
    }

    #endregion

    #region IList Member

    /// <inheritdoc cref="DbParameterCollection.IsReadOnly" />
    public override bool IsReadOnly => false;

    /// <summary>
    /// Removes the parameter from the collection using the specified index.
    /// </summary>
    /// <param name="index">The zero-based index of the parameter.</param>
    public override void RemoveAt(int index)
    {
        if (_parameters.Count - 1 < index)
            throw new ArgumentOutOfRangeException(nameof(index));

        Remove(_parameters[index]);
    }

    /// <inheritdoc cref="DbParameterCollection.Insert" />
    public override void Insert(int index, object? value)
    {
        _parameters.Insert(index, ParameterItem.Create(value));
    }

    /// <summary>
    /// Removes the parameter specified by the parameterName from the collection.
    /// </summary>
    /// <param name="parameterName">The name of the parameter to remove from the collection.</param>
    public void Remove(string parameterName)
    {
        if (parameterName is null)
            throw new ArgumentNullException(nameof(parameterName));

        var index = IndexOf(parameterName);
        if (index < 0)
            throw new InvalidOperationException("No parameter with the specified name exists in the collection");

        RemoveAt(index);
    }

    /// <inheritdoc cref="DbParameterCollection.Remove(object)" />
    public override void Remove(object? value)
    {
        if (value is null)
            throw new ArgumentNullException(nameof(value));

        for (var i = 0; i < _parameters.Count; i++)
        {
            var p = _parameters[i];
            if (p.Value == value)
                _parameters.RemoveAt(i);
        }
    }

    /// <inheritdoc cref="DbParameterCollection.Contains(object)" />
    public override bool Contains(object? value) => IndexOf(value) != -1;

    /// <summary>
    /// Gets a value indicating whether a <see cref="NpgsqlParameter"/> with the specified parameter name exists in the collection.
    /// </summary>
    /// <param name="parameterName">The name of the <see cref="NpgsqlParameter"/> object to find.</param>
    /// <param name="parameter">
    /// A reference to the requested parameter is returned in this out param if it is found in the list.
    /// This value is <see langword="null"/> if the parameter is not found.
    /// </param>
    /// <returns>
    /// <see langword="true"/> if the collection contains the parameter and param will contain the parameter;
    /// otherwise, <see langword="false"/>.
    /// </returns>
    public bool TryGetValue(string parameterName, [NotNullWhen(true)] out NpgsqlDbParameter? parameter)
    {
        if (parameterName is null)
            throw new ArgumentNullException(nameof(parameterName));

        var index = IndexOf(parameterName);

        if (index != -1)
        {
            parameter = GetOrAddNpgsqlParameter(index);
            return true;
        }

        parameter = null;
        return false;
    }


    /// <summary>
    /// Gets a value indicating whether a <see cref="NpgsqlParameter"/> with the specified parameter name exists in the collection.
    /// </summary>
    /// <param name="parameterName">The name of the <see cref="NpgsqlParameter"/> object to find.</param>
    /// <param name="value">
    /// A reference to the requested parameter is returned in this out param if it is found in the list.
    /// This value is <see langword="null"/> if the parameter is not found.
    /// </param>
    /// <returns>
    /// <see langword="true"/> if the collection contains the parameter and param will contain the parameter;
    /// otherwise, <see langword="false"/>.
    /// </returns>
    public bool TryGetValue(string parameterName, out object? value)
    {
        if (parameterName is null)
            throw new ArgumentNullException(nameof(parameterName));

        var index = IndexOf(parameterName);

        if (index != -1)
        {
            var p = _parameters[index];
            value = p.Value;
            return true;
        }

        value = default;
        return false;
    }

    /// <summary>
    /// Removes all items from the collection.
    /// </summary>
    public override void Clear()
    {
        _parameters.Clear();
        LookupClear();
    }

    /// <inheritdoc cref="DbParameterCollection.IndexOf(object)" />
    public override int IndexOf(object? value)
    {
        if (value is null)
            throw new ArgumentNullException(nameof(value));

        for (var i = 0; i < _parameters.Count; i++)
        {
            var p = _parameters[i];
            if (p.Value == value)
                return i;
        }

        return -1;
    }

    /// <inheritdoc cref="DbParameterCollection.Add" />
    public override int Add(object? value)
    {
        _parameters.Add(ParameterItem.Create(value));
        return Count - 1;
    }

    /// <inheritdoc />
    public override bool IsFixedSize => false;

    #endregion

    #region ICollection Member

    /// <inheritdoc />
    public override bool IsSynchronized => false;

    /// <summary>
    /// Gets the number of <see cref="NpgsqlParameter"/> objects in the collection.
    /// </summary>
    /// <value>The number of <see cref="NpgsqlParameter"/> objects in the collection.</value>
    public override int Count => _parameters.Count;

    /// <inheritdoc />
    public override void CopyTo(Array array, int index)
    {
        if (index >= 0 && index + _parameters.Count <= array.Length)
            throw new ArgumentOutOfRangeException(nameof(array), "Array too small.");

        var list = array as IList;
        for (var i = 0; i < _parameters.Count; i++)
        {
            list[index + i] = GetOrAddNpgsqlParameter(i);
        }
    }

    /// <inheritdoc />
    bool ICollection<NpgsqlDbParameter>.IsReadOnly => false;

    /// <inheritdoc />
    public override object SyncRoot => _parameters;

    #endregion

    #region IEnumerable Member

    IEnumerator<NpgsqlDbParameter> IEnumerable<NpgsqlDbParameter>.GetEnumerator()
    {
        for (var i = 0; i < _parameters.Count; i++)
            yield return GetOrAddNpgsqlParameter(i);
    }

    IEnumerator<KeyValuePair<string, object?>> IEnumerable<KeyValuePair<string, object?>>.GetEnumerator()
        => GetFastEnumerator();

    // Beautiful ADO.NET design to hog the public GetEnumerator method slot for a non generic IEnumerable method...
    internal Enumerator GetFastEnumerator() => new(_parameters);

    public class Enumerator : IEnumerator<KeyValuePair<string, object?>>
    {
        readonly ImmutableArray<ParameterItem>.Builder _parameters;
        int _index;
        KeyValuePair<string, object?> _current;

        internal Enumerator(ImmutableArray<ParameterItem>.Builder parameters)
        {
            _parameters = parameters;
        }

        public bool MoveNext()
        {
            var parameters = _parameters;

            if ((uint)_index < (uint)parameters.Count)
            {
                _current = parameters[_index].AsKeyValuePair();
                _index++;
                return true;
            }

            _current = default;
            _index = parameters.Count + 1;
            return false;
        }

        public KeyValuePair<string, object?> Current => _current;

        public void Reset()
        {
            _index = 0;
            _current = default;
        }

        public Enumerator GetEnumerator() => new(_parameters);

        object IEnumerator.Current => Current;
        public void Dispose() { }
    }

    /// <inheritdoc />
    public override IEnumerator GetEnumerator() => new Enumerator(_parameters);

    #endregion

    /// <inheritdoc />
    public override void AddRange(Array values)
    {
        if (values is null)
            throw new ArgumentNullException(nameof(values));

        foreach (var parameter in values)
            Add(parameter);
    }

    /// <inheritdoc />
    protected override DbParameter GetParameter(string parameterName) => this[parameterName];

    /// <inheritdoc />
    protected override DbParameter GetParameter(int index) => this[index];

    /// <inheritdoc />
    protected override void SetParameter(string parameterName, DbParameter value)
        => ((IDataParameterCollection)this)[parameterName] = value;

    /// <inheritdoc />
    protected override void SetParameter(int index, DbParameter value) => ((IList)this)[index] = value;

    /// <summary>
    /// Report the offset within the collection of the given parameter.
    /// </summary>
    /// <param name="value">Parameter to find.</param>
    /// <returns>Index of the parameter, or -1 if the parameter is not present.</returns>
    public int IndexOf(NpgsqlDbParameter value) => IndexOf((object)value);

    /// <summary>
    /// Insert the specified parameter into the collection.
    /// </summary>
    /// <param name="index">Index of the existing parameter before which to insert the new one.</param>
    /// <param name="value">Parameter to insert.</param>
    public void Insert(int index, NpgsqlDbParameter value)
    {
        if (value is null)
            throw new ArgumentNullException(nameof(value));

        var item = ParameterItem.Create(value.ParameterName, value);
        _parameters.Insert(index, item);
        if (!item.IsPositional)
            LookupInsert(item.Name, index);
    }

    /// <summary>
    /// Report whether the specified parameter is present in the collection.
    /// </summary>
    /// <param name="value">Parameter to find.</param>
    /// <returns>True if the parameter was found, otherwise false.</returns>
    public bool Contains(NpgsqlDbParameter value) => Contains((object)value);

    /// <summary>
    /// Remove the specified parameter from the collection.
    /// </summary>
    /// <param name="value">Parameter to remove.</param>
    /// <returns>True if the parameter was found and removed, otherwise false.</returns>
    public bool Remove(NpgsqlDbParameter value)
    {
        if (value == null)
            throw new ArgumentNullException(nameof(value));

        var index = IndexOf(value);
        if (index >= 0)
        {
            var pItem = _parameters[index];
            _parameters.RemoveAt(index);
            if (!LookupEnabled)
                LookupClear();
            if (!pItem.IsPositional)
                LookupRemove(pItem.Name, index);
            return true;
        }

        return false;
    }

    /// <summary>
    /// Convert collection to a System.Array.
    /// </summary>
    /// <param name="array">Destination array.</param>
    /// <param name="arrayIndex">Starting index in destination array.</param>
    public void CopyTo(NpgsqlDbParameter[] array, int arrayIndex) => CopyTo((Array)array, arrayIndex);

    /// <summary>
    /// Convert collection to a System.Array.
    /// </summary>
    /// <returns>NpgsqlParameter[]</returns>
    public NpgsqlDbParameter[] ToArray() => System.Linq.Enumerable.ToArray<NpgsqlDbParameter>(this);

}
