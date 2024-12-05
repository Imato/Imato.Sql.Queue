namespace Imato.Sql.Queue
{
    public interface IActionQueueService
    {
        Task<int> AddActionAsync(ActionQueue action);

        Task AddActionsAsync(ActionQueue[] actions);

        Task ClearOldAsync();

        Task ClearStartedActionAsync();

        Task CreateTableAsync();

        Task<IEnumerable<ActionQueue>> GetNewActionsAsync();

        Task StartActionAsync(ActionQueue action, CancellationToken? cancellationToken = null);

        Task<ActionQueue?> GetActionAsync(int id);

        Task UpdateActionAsync(ActionQueue action);

        Task ProcessQueueAsync(CancellationToken token);

        // Add mapping fanction name - to executed function
        void AddFunction(string name, Func<Dictionary<string, string>, CancellationToken, Task> func);
    }
}