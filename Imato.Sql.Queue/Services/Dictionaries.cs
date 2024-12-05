namespace Imato.Sql.Queue
{
    internal static class Dictionaries
    {
        public static V? GetValue<K, V>(this IDictionary<K, V> dictionary, K key)
        {
            if (dictionary.ContainsKey(key)) return dictionary[key];
            return default;
        }
    }
}