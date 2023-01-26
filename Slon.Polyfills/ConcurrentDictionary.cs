using System;
using System.Collections.Concurrent;

namespace System.Collections.Concurrent;

#if NETSTANDARD2_0
public static class ConcurrentDictionaryExtensions
{
    public static TValue GetOrAdd<TKey,TValue,TArg>(this ConcurrentDictionary<TKey,TValue> instance, TKey key, Func<TKey, TArg, TValue> valueFactory, TArg factoryArgument)
        => instance.GetOrAdd(key, key => valueFactory(key, factoryArgument));
}
#endif
