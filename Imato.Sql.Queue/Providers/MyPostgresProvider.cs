using Dapper;
using Imato.Dapper.DbContext;

namespace Imato.Sql.Queue
{
    internal class MyPostgresProvider : PostgresProvider, IMyProvider
    {
        private static string[] columns = ["action", "actionGroup", "actionType", "dt", "priority", "timeOut", "source"];

        public MyPostgresProvider(string? connectionString = null) : base(connectionString)
        {
        }

        public string TableName => "actions.queue";

        public async Task CreateTableAsync()
        {
            const string sql =
@"
create schema if not exists actions;

create table if not exists actions.queue (
	id serial not null primary key,
	dt timestamp not null,
	action text not null,
	processDt timestamp,
	duration int,
	isDone boolean not null default (false),
	isStarted boolean not null default (false),
	error text,
	actionType varchar(10) not null,
	source varchar(255),
	actionGroup varchar(25),
	priority int,
    timeOut int);

create index if not exists actions_queue_isDone_ix on actions.queue (isDone);
create index if not exists actions_queue_dt_ix on actions.queue (dt);";

            using var c = CreateConnection();
            await c.ExecuteAsync(sql: string.Format(sql, TableName));
        }

        public async Task<IEnumerable<ActionQueue>> GetActionsAsync(int count)
        {
            const string sql =
@"create temp table if not exists tmp_actions
	(id int, action text, actionType varchar(10), priority int, timeOut int);

truncate tmp_actions;

insert into tmp_actions
select max(q.id) id, q.action, q.actionType, q.priority, coalesce(max(q.timeOut), 0) timeOut
  from {0} q
  where q.isStarted = false
    and q.isDone = false
    and 10 * 2 >
    	(select count(1)
			  from {0} q
			  where q.isStarted = true
			    and q.isDone = false
			    and q.processDt > now() - 10 * interval'1 minute')
  group by q.action, q.actionType, q.priority
  order by priority, id
 	limit 10;

update {0} a
  set isDone = true,
      error = 'Doubles',
      processDt = now(),
      duration = 0,
      isStarted = false
  from tmp_actions t
  where t.action = a.action
    and t.id != a.id
    and a.isDone = false
    and a.isStarted = false;

select id, action, actionType, priority, timeOut from tmp_actions order by id;";

            using var c = CreateConnection();
            return await c.QueryAsync<ActionQueue>(
                sql: string.Format(sql, TableName),
                param: new { count });
        }

        public async Task<int> AddActionAsync(ActionQueue action)
        {
            const string sql =
@"insert into {0} (action, dt, actionType, source, actionGroup, priority, timeOut)
values (@action, @dt, @actionType, @source, @actionGroup, @priority, @timeOut)
returning id; ";

            using var c = CreateConnection();
            return await c.QuerySingleAsync<int>(sql: string.Format(sql, TableName),
                    param: action);
        }

        public async Task AddActionsAsync(ActionQueue[] actions)
        {
            using var c = CreateConnection();
            await c.BulkInsertAsync(actions, TableName, columns, skipFieldsCheck: false);
        }

        public async Task ClearStartedActionAsync()
        {
            const string sql =
@"update {0}
    set processTimeSec = null,
        msg = null,
        isStarted = false
    where isStarted = true
        and isDone = false";
            using var c = CreateConnection();
            await c.ExecuteAsync(sql: string.Format(sql, TableName), commandTimeout: 60);
        }

        public async Task StartActionAsync(ActionQueue action)
        {
            const string sql =
@"update {0}
    set processDt = now(),
        isDone = false,
        isStarted = true
    where id = @id;";
            using var c = CreateConnection();
            await c.ExecuteAsync(string.Format(sql, TableName), action);
        }

        public async Task EndActionAsync(ActionQueue action)
        {
            const string sql =
@"update {0}
    set duration = @duration,
        error = @error,
        isDone = @isDone,
        isStarted = 0
    where id = @id;";

            using var c = CreateConnection();
            await c.ExecuteAsync(string.Format(sql, TableName), action);
        }

        public async Task CancelActionAsync(int actionId)
        {
            const string sql =
@"update {0}
    set duration = date_part('millisecond', (processDt - now())),
        error = 'Cancel ended action after timeout',
        isDone = true,
        isStarted = false
    where id = @actionId
        and isDone = false;";
            using var c = CreateConnection();
            await c.ExecuteAsync(sql: string.Format(sql, TableName), new { actionId });
        }

        public async Task ClearOldAsync()
        {
            const string sql = "delete from {0} where isDone = 1 and dt < getdate() - 7;";

            using var c = CreateConnection();
            await c.ExecuteAsync(sql: string.Format(sql, TableName));
        }

        public async Task<ActionQueue?> GetActionAsync(int id)
        {
            const string sql = "select * from {0} where id = @id;";
            using var c = CreateConnection();
            return await c.QuerySingleOrDefaultAsync<ActionQueue>(sql: string.Format(sql, TableName),
                param: new { id });
        }
    }
}