namespace Imato.Sql.Queue
{
    public class QueueSettings
    {
        /// <summary>
        /// Max threads count
        /// </summary>
        public int Threads { get; set; } = Environment.ProcessorCount;

        /// <summary>
        /// Retry action execution count
        /// </summary>
        public byte RetryActionCount { get; set; } = 3;

        public TimeSpan RetryDelay { get; set; } = TimeSpan.FromMilliseconds(123);
        public TimeSpan DefaultExecutionTimeout { get; set; } = TimeSpan.FromMinutes(30);

        /// <summary>
        /// Queue store
        /// </summary>
        public string ConnectionStringName { get; set; } = "";

        /// <summary>
        /// Functions for execute in queue.
        /// For example: { "MyFunctionName", async () => await sameClass.Method(parameters) }
        /// Add to queue new action "MyFunctionName @parameter1 = 1, @parameter2 = ""new value"""
        /// </summary>
        public Dictionary<string, Func<Dictionary<string, string>, Task>> Functions { get; set; } = new();
    }
}