using Microsoft.Extensions.Logging;
using Imato.Logger.Extensions;
using Imato.Try;
using System.Diagnostics;
using Dapper;

namespace Imato.Sql.Queue

{
    public class ActionQueueService : IActionQueueService
    {
        private DateTime _lastClear;
        private readonly IActionQueueRepository _repository;
        private readonly IMyProvider _dbPovider;
        private readonly QueueSettings _settings;
        private readonly ILogger<ActionQueueService> _logger;
        private readonly Dictionary<ActionQueue, Task> _actions = new();
        private static byte _byte = 1;
        private DateTime _cancelDate;

        public ActionQueueService(IActionQueueRepository repository,
            QueueSettings settings,
            ILogger<ActionQueueService> logger)
        {
            _repository = repository;
            _settings = settings;
            _dbPovider = _repository.GetProvider();
            _logger = logger;
        }

        public Task<IEnumerable<ActionQueue>> GetNewActionsAsync()
        {
            _logger?.LogDebug(() => "Get new actions");
            return _dbPovider.GetActionsAsync(_settings.Threads);
        }

        public async Task StartActionAsync(ActionQueue action)
        {
            _logger?.LogDebug(() => "Try start action {0}", [action]);
            var watch = new Stopwatch();
            watch.Start();

            while (action.AttemptCount <= _settings.MaxAttemptCount)
            {
                var isDone = false;

                try
                {
                    action.AttemptCount++;
                    await TryAsync(() => _dbPovider.StartActionAsync(action));

                    switch (action.ActionType)
                    {
                        case ActionQueue.ActionTypeSql:
                            await StartSqlActionAsync(action);
                            break;

                        case ActionQueue.ActionTypeNet:
                            await StartDotNetActionAsync(action);
                            break;

                        default:
                            action.AttemptCount = _settings.MaxAttemptCount;
                            action.Error = $"Unknown action type {action.ActionType}";
                            break;
                    }

                    isDone = true;
                    action.AttemptCount = (byte)(_settings.MaxAttemptCount + _byte);
                }
                catch (Exception e)
                {
                    if (action.AttemptCount >= _settings.MaxAttemptCount)
                    {
                        action.Error = e.ToString();
                        _logger?.LogError(() => $"Run {action.ActionType}: {action.Action} \n error: {e}");
                    }
                }

                watch.Stop();
                action.IsDone = isDone || action.AttemptCount >= _settings.MaxAttemptCount;
                action.Duration = watch.ElapsedMilliseconds;

                await TryAsync(() => _dbPovider.EndActionAsync(action));
            }
        }

        protected async Task StartSqlActionAsync(ActionQueue action)
        {
            var cs = action.Action.Split(']');
            var connectionString = cs.Length == 1 ? "" : cs[0];

            var parameters = Strings.ParseParameters(action.Action);
            var command = action.Action;
            var timeOut = (action?.TimeOut ?? 600_000) / 1_000;

            using (var connection = _repository.Connection(connectionString))
            {
                if (int.TryParse(parameters.GetValue("timeOut"), out int t))
                {
                    timeOut = t;
                    command = command.Replace($", @timeOut = {timeOut}", "", StringComparison.InvariantCultureIgnoreCase);
                }

                await connection.ExecuteAsync(sql: command, commandTimeout: timeOut);
            }
        }

        protected Task StartDotNetActionAsync(ActionQueue action)
        {
            var parameters = Strings.ParseParameters(action.Action);
            var functionName = action.Action.Split(' ').FirstOrDefault() ?? "unknown";
            if (!_settings.Functions.ContainsKey(functionName))
            {
                action.AttemptCount = _settings.MaxAttemptCount;
                action.Error = ($"{functionName} is not registered in QueueSettings");
            }
            var function = _settings.Functions[functionName];
            return function(parameters);
        }

        private async Task WaitAsync(ActionQueue action)
        {
            if (!action.IsDone)
            {
                var delay = (action.Error?.Contains("BadGateway", StringComparison.InvariantCultureIgnoreCase) == true) ? 3000 : 500;
                await Task.Delay(delay);
            }
        }

        public async Task ClearOldAsync()
        {
            if ((DateTime.Now - _lastClear).TotalHours < 24) return;
            _lastClear = DateTime.Now;
            _logger?.LogDebug(() => "Clear old actions");
            await _dbPovider.ClearOldAsync();
        }

        public async Task ClearStartedActionAsync()
        {
            _logger?.LogDebug(() => "Clear started");
            await _dbPovider.ClearStartedActionAsync();
        }

        public Task<int> AddActionAsync(ActionQueue action)
        {
            _logger?.LogDebug(() => "Add new action {0}", [action]);
            return _dbPovider.AddActionAsync(action);
        }

        public async Task AddActionsAsync(ActionQueue[] actions)
        {
            if (actions == null || actions.Length == 0) return;

            _logger?.LogDebug(() => "Add new actions {0} count", [actions.Length]);

            if (actions.Length == 1)
            {
                await AddActionAsync(actions[0]);
            }
            else
            {
                await _dbPovider.AddActionsAsync(actions);
            }
        }

        public Task<ActionQueue?> GetActionAsync(int id)
        {
            return _dbPovider.GetActionAsync(id);
        }

        public Task CreateTableAsync()
        {
            _logger?.LogDebug(() => "Create actions table");
            return _dbPovider.CreateTableAsync();
        }

        public void AddFunction(string name, Func<Dictionary<string, string>, Task> func)
        {
            _settings.Functions.Add(name, func);
        }

        public async Task ProcessQueueAsync(CancellationToken token)
        {
            await ClearOldAsync();
            var newActions = await GetNewActionsAsync();

            foreach (var action in newActions)
            {
                var task = new Task(async () =>
                    {
                        var a = action;
                        await StartActionAsync(a);
                    },
                    token);
                _actions.TryRemove(action)?.Dispose();
                _actions.Add(action, task);
                task.Start();
                Thread.Sleep(10);
            }

            await CancelOldAsync();
        }

        public async Task CancelOldAsync()
        {
            var now = DateTime.Now;
            foreach (var action in _actions
                   .Where(x => (x.Key.TimeOut > 0 && x.Key.Dt < now.AddMilliseconds(-1 * x.Key.TimeOut.Value)))
                   // || x.Value.IsCompleted )
                   .Select(x => x.Key)
                   .ToArray())
            {
                _actions.TryRemove(action)?.Dispose();
                await _dbPovider.CancelActionAsync(action.Id);
            }
        }

        private async Task TryAsync(Func<Task> func)
        {
            await Try.Try.Function(func)
                .Setup(new TryOptions
                {
                    Delay = 200,
                    ErrorOnFail = true,
                    RetryCount = 3,
                    Timeout = 30_000
                })
                .OnError((ex) => _logger?.LogError(ex, () => "Execution error"))
                .ExecuteAsync();
        }
    }
}