using Imato.Dapper.DbContext;

namespace Imato.Sql.Queue
{
    public interface IMyProvider : IContextProvider
    {
        public Task CreateTableAsync();

        Task<IEnumerable<ActionQueue>> GetActionsAsync(int count);

        Task<int> AddActionAsync(ActionQueue action);

        Task AddActionsAsync(ActionQueue[] actions);

        Task ClearStartedActionAsync();

        Task StartActionAsync(ActionQueue action);

        string TableName { get; }

        Task EndActionAsync(ActionQueue action);

        Task CancelActionAsync(int actionId);

        Task ClearOldAsync();

        Task<ActionQueue?> GetActionAsync(int id);
    }
}