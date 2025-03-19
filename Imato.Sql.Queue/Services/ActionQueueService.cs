using Microsoft.Extensions.Logging;
using Imato.Logger.Extensions;
using Imato.Try;
using System.Diagnostics;
using Dapper;
using System.Collections.Concurrent;

namespace Imato.Sql.Queue

{
    public class ActionQueueService : IActionQueueService, IDisposable
    {
        private DateTime _lastClear = DateTime.Now.AddHours(-23);
        private readonly IActionQueueRepository _repository;
        private readonly IMyProvider _dbPovider;
        private readonly QueueSettings _settings;
        private readonly ILogger<ActionQueueService> _logger;
        private readonly ConcurrentDictionary<ActionQueue, Task> _actions = new();
        private Task? _cleaningTask = null;
        private CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
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

        public void StartCleaning()
        {
            if (_cleaningTask == null)
            {
                var token = _cancellationTokenSource.Token;
                _cleaningTask = Task.Factory.StartNew(async (_) =>
                {
                    while (!token.IsCancellationRequested)
                    {
                        await CancelOldAsync(token);
                        await ClearOldAsync();
                        await Task.Delay(180_000);
                    }
                },
                TaskCreationOptions.LongRunning,
                token);
            }
        }

        public void StopCleaning()
        {
            if (!_cancellationTokenSource.IsCancellationRequested)
                _cancellationTokenSource.Cancel();
        }

        public Task<IEnumerable<ActionQueue>> GetNewActionsAsync()
        {
            _logger?.LogDebug(() => "Get new actions");
            return _dbPovider.GetActionsAsync(_settings.Threads);
        }

        public async Task StartActionAsync(ActionQueue action, CancellationToken? cancellationToken = null)
        {
            _logger?.LogDebug(() => "Try start action {0}", [action]);
            var watch = new Stopwatch();
            watch.Start();

            action.IsStarted = true;
            action.IsDone = false;
            action.ProcessDt = DateTime.Now;
            await TryAsync(() => _dbPovider.UpdateAsync(action));

            var timeOut = action.TimeOut > 0
                    ? TimeSpan.FromMilliseconds(action.TimeOut.Value)
                    : _settings.DefaultExecutionTimeout;

            while (action.AttemptCount <= _settings.RetryActionCount)
            {
                try
                {
                    action.AttemptCount++;

                    switch (action.ActionType)
                    {
                        case ActionQueue.ActionTypeSql:
                            await StartSqlActionAsync(action);
                            break;

                        case ActionQueue.ActionTypeNet:
                            await StartDotNetActionAsync(action, cancellationToken ?? new CancellationTokenSource(timeOut).Token);
                            break;

                        default:
                            action.AttemptCount = _settings.RetryActionCount;
                            action.Error = $"Unknown action type {action.ActionType}";
                            break;
                    }

                    action.AttemptCount = (byte)(_settings.RetryActionCount + _byte);
                }
                catch (Exception e)
                {
                    _logger?.LogError(e, $"Run {action.ActionType}: {action.Action} \n error");
                    action.Error = e.ToString();
                    action.Duration = watch.ElapsedMilliseconds;
                    await TryAsync(() => _dbPovider.UpdateAsync(action));
                    if (_settings.RetryDelay.TotalMilliseconds > 0
                        && action.AttemptCount < _settings.RetryActionCount)
                        await Task.Delay(_settings.RetryDelay);
                }
            }

            watch.Stop();
            action.IsDone = true;
            action.IsStarted = false;
            action.Duration = watch.ElapsedMilliseconds;
            await TryAsync(() => _dbPovider.UpdateAsync(action));
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

        protected Task StartDotNetActionAsync(ActionQueue action, CancellationToken cancellationToken)
        {
            var parameters = Strings.ParseParameters(action.Action);
            var functionName = action.Action.Split(' ').FirstOrDefault() ?? "unknown";
            if (!_settings.Functions.ContainsKey(functionName))
            {
                action.AttemptCount = _settings.RetryActionCount;
                action.Error = ($"{functionName} is not registered in QueueSettings");
            }
            var function = _settings.Functions[functionName];
            return function(parameters, cancellationToken);
        }

        public async Task ClearOldAsync()
        {
            if ((DateTime.Now - _lastClear).TotalHours < 24)
                return;
            _lastClear = DateTime.Now;
            _logger?.LogDebug(() => "Clear old actions");
            await _dbPovider.ClearOldAsync(_settings.ClearQueueAfterDays);
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

        public Task UpdateActionAsync(ActionQueue action)
        {
            return _dbPovider.UpdateAsync(action);
        }

        public Task CreateTableAsync()
        {
            _logger?.LogDebug(() => "Create actions table");
            return _dbPovider.CreateTableAsync();
        }

        public void AddFunction(string name, Func<Dictionary<string, string>, CancellationToken, Task> func)
        {
            _settings.Functions.Add(name, func);
        }

        public async Task ProcessQueueAsync(CancellationToken token)
        {
            var newActions = await GetNewActionsAsync();

            foreach (var action in newActions)
            {
                var task = new Task(async () =>
                    {
                        var a = action;
                        await StartActionAsync(a);
                        _actions.TryRemove(a)?.Dispose();
                    },
                    token);
                try
                {
                    _actions.TryRemove(action)?.Dispose();
                    _actions.TryAdd(action, task);
                }
                catch { }

                task.Start();
                Thread.Sleep(10);
            }
        }

        public async Task CancelOldAsync(CancellationToken token)
        {
            if ((DateTime.Now - _cancelDate) < TimeSpan.FromMinutes(10))
            {
                return;
            }

            /*
            // Stop process after timeout
            foreach (var action in _actions
                       .Where(x => !x.Key.IsDone && x.Key.IsStarted && IsTimedOut(x.Key))
                       // || x.Value.IsCompleted )
                       .Select(x => x.Key)
                       .ToArray())
            {
                try
                {
                    action.Duration = ActionDuration(action);
                    var timeOut = action.TimeOut > 0 ? action.TimeOut.Value : _settings.DefaultExecutionTimeout.TotalMilliseconds;
                    _actions.TryRemove(action)?.Dispose();
                    action.IsDone = true;
                    action.IsStarted = false;
                    action.Error = $"{DateTime.Now:HH:mm:ss}: Cancel action after timeout: {timeOut}, duration: {action.Duration} ";
                    await _dbPovider.UpdateAsync(action);
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, $"Cancel old queue error");
                }
            }
            */

            // Delete finished
            foreach (var action in _actions
                       .Select(x => x.Key)
                       .Where(x => x.IsDone)
                       .ToArray())
            {
                _actions.TryRemove(action)?.Dispose();
            }

            _cancelDate = DateTime.Now;
        }

        private long ActionDuration(ActionQueue action)
        {
            return (long)(DateTime.Now - (action.ProcessDt ?? action.Dt)).TotalMilliseconds;
        }

        private bool IsTimedOut(ActionQueue action)
        {
            return ActionDuration(action) > (action.TimeOut > 0 ? action.TimeOut.Value : _settings.DefaultExecutionTimeout.TotalMilliseconds);
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
                .OnError((ex) => _logger?.LogError(ex, "Execution error"))
                .ExecuteAsync();
        }

        public void Dispose()
        {
            _cancelDate = DateTime.MinValue;
            CancelOldAsync(CancellationToken.None).Wait();
            _cancellationTokenSource.Cancel();
        }
    }
}