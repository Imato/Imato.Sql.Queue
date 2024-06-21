namespace Imato.Sql.Queue
{
    internal static class Dictionaries
    {
        public static V? GetValue<K, V>(this IDictionary<K, V> dictionary, K key)
        {
            if (dictionary.ContainsKey(key)) return dictionary[key];
            return default;
        }

        public static T? GetValue<T>(this IDictionary<string, T> dictionary, string key)
        {
            var kv = dictionary
                .Where(x => x.Key.Equals(key, StringComparison.OrdinalIgnoreCase));
            if (kv.Any()) return kv.First().Value;
            return default;
        }
    }
}