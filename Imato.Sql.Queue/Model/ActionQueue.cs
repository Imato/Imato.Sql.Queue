namespace Imato.Sql.Queue
{
    public class ActionQueue
    {
        public int Id { get; set; }
        public string Action { get; set; } = null!;
        public string? Error { get; set; }
        public long Duration { get; set; }
        public DateTime Dt { get; set; } = DateTime.Now;
        public bool IsDone { get; set; }
        public bool IsStarted { get; set; }
        public string ActionType { get; set; } = ActionTypeNet;
        public string? Source { get; set; }
        public byte AttemptCount { get; set; }
        public string? ActionGroup { get; set; }
        public int Priority { get; set; }
        public int? TimeOut { get; set; }

        public const string ActionTypeSql = "sql";

        public const string ActionTypeNet = ".net";

        public override bool Equals(object? obj)
        {
            return (obj as ActionQueue)?.Id == Id;
        }

        public override int GetHashCode()
        {
            return Id + 432921122;
        }
    }
}