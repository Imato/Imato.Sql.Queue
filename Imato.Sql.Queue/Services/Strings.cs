namespace Imato.Sql.Queue
{
    internal static class Strings
    {
        public static Dictionary<string, string> ParseParameters(string str)
        {
            var result = new Dictionary<string, string>();
            var args = str.Split("@");
            for (int i = 0; i < args.Length; i++)
            {
                var sp = args[i].IndexOf("=");
                if (sp > 0)
                {
                    var key = args[i]
                        .Substring(0, sp)
                        .Trim();
                    var value = args[i]
                        .Substring(sp + 1, args[i].Length - sp - 1)
                        .Replace(",", "")
                        .Replace("'", "")
                        .Trim();
                    value = value.StartsWith("\"") && value.EndsWith("\"")
                        ? args[i].Substring(sp + 2, args[i].Length - sp - 3)
                        : value;
                    result.Add(key, value);
                }
            }
            return result;
        }
    }
}