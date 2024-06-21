namespace Imato.Sql.Queue
{
    public class QueueSettings
    {
        public int Threads { get; set; } = Environment.ProcessorCount;
        public byte MaxAttemptCount { get; set; } = 3;
        public string ConnectionStringName { get; set; } = "";

        /// <summary>
        /// Functions for execute in queue.
        /// For example: { "MyFunctionName", async () => await sameClass.Method(parameters) }
        /// Add to queue new action "MyFunctionName @parameter1 = 1, @parameter2 = ""new value"""
        /// </summary>
        public Dictionary<string, Func<Dictionary<string, string>, Task>> Functions { get; set; } = new();
    }
}