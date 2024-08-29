namespace Imato.Sql.Queue
{
    internal static class Dictionary
    {
        public static V? TryRemove<K, V>(this IDictionary<K, V> dic, K key)
        {
            if (dic.TryGetValue(key, out var value))
            {
                dic.Remove(key);
                return value;
            }
            return default;
        }
    }
}