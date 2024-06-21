using Microsoft.Extensions.Logging;
using Imato.Logger.Extensions;
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
        private readonly List<Task> _actions = new();
        private static byte _byte = 1;

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
                    await _dbPovider.StartActionAsync(action);

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
                    action.Error = string.Empty;
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

                try
                {
                    await _dbPovider.EndActionAsync(action);
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, () => "EndActionAsync");
                }
                await WaitAsync(action);
            }
        }

        protected async Task StartSqlActionAsync(ActionQueue action)
        {
            var cs = action.Action.Split(']');
            var connectionString = cs.Length == 1 ? "" : cs[0];

            var parameters = Strings.ParseParameters(action.Action);
            var command = action.Action;

            using (var connection = _repository.Connection(connectionString))
            {
                if (int.TryParse(parameters.GetValue("timeOut"), out int timeOut))
                {
                    command = command.Replace($", @timeOut = {timeOut}", "", StringComparison.InvariantCultureIgnoreCase);
                    await connection.ExecuteAsync(sql: command, commandTimeout: timeOut);
                    return;
                }

                await connection.ExecuteAsync(sql: command, commandTimeout: 600);
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
                _actions.Add(task);
                task.Start();
                Thread.Sleep(10);
            }

            foreach (var action in _actions
                                    .Where(x => x.Status == TaskStatus.WaitingForActivation)
                                    .ToArray())
            {
                _actions.Remove(action);
            }
        }
    }
}