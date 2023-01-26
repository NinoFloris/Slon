namespace System.Collections.Generic;

#if NETSTANDARD2_0
static class KeyValuePairExtensions
{
    public static void Deconstruct<TKey, TValue>(this KeyValuePair<TKey, TValue> kv, out TKey key, out TValue value)
    {
        key = kv.Key;
        value = kv.Value;
    }
}
#endif
