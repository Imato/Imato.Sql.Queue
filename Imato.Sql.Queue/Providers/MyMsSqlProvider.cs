using Dapper;
using Imato.Dapper.DbContext;

namespace Imato.Sql.Queue
{
    internal class MyMsSqlProvider : MsSqlProvider, IMyProvider
    {
        public MyMsSqlProvider(string? connectionString = null) : base(connectionString)
        {
        }

        public string TableName => "actions.queue";

        public async Task CreateTableAsync()
        {
            const string sql =
@"
if schema_id('actions') is null
	exec sp_executesql N'create schema actions';

if (object_id('{0}') is null)
begin
    CREATE TABLE {0} (
        [id]             INT             IDENTITY (1, 1) NOT NULL,
        [dt]             DATETIME        CONSTRAINT [ActionQueue_dt_DF] DEFAULT (getdate()) NOT NULL,
        [action]         NVARCHAR (4000) NOT NULL,
        [processDt]      DATETIME        NULL,
        [duration]       INT             NULL,
        [isDone]         BIT             CONSTRAINT [ActionQueue_isDone_DF] DEFAULT ((0)) NOT NULL,
        [error]          VARCHAR (MAX)   NULL,
        [actionType]     VARCHAR (10)    NULL,
        [source]         VARCHAR (255)   NULL,
        [isStarted]      BIT             CONSTRAINT [ActionQueue_isStarted_DF] DEFAULT ((0)) NOT NULL,
        [actionGroup]    VARCHAR (25)    NULL,
        [priority]       INT             NULL
        CONSTRAINT [ActionQueue_PK] PRIMARY KEY CLUSTERED ([id] ASC)
    );

    CREATE INDEX [ActionQueue_isDone_IX]
        ON {0}([isDone] ASC);

    CREATE NONCLUSTERED INDEX [ActionQueue_dt_IX]
        ON {0}([dt] ASC);
end";

            using var c = CreateConnection();
            await c.ExecuteAsync(sql: string.Format(sql, TableName));
        }

        public async Task<IEnumerable<ActionQueue>> GetActionsAsync(int count)
        {
            const string sql =
@"declare @started int;

select @started = count(1)
  from {0} q (nolock)
  where q.isStarted = 1
    and q.isDone = 0
    and q.processDt > dateadd(minute, -10, getdate());

declare @actions table
	(id int, action nvarchar(4000), actionType varchar(10), priority int);

insert into @actions
select top (@count) max(q.id) id, q.action, q.actionType, q.priority
  from {0} q
  where q.isStarted = 0
    and q.isDone = 0
    and @started < @count * 2
  group by q.action, q.actionType, q.priority
  order by priority, id;

update a
    set isDone = 1,
        error = 'Doubles',
        processDt = getdate(),
        duration = 0,
        isStarted = 0
  from {0} a
    join @actions t
      on t.action = a.action
      and t.id != a.id
      and a.isDone = 0;

select id, action, actionType, priority from @actions order by id;";

            using var c = CreateConnection();
            return await c.QueryAsync<ActionQueue>(
                sql: string.Format(sql, TableName),
                param: new { count });
        }

        public async Task<int> AddActionAsync(ActionQueue action)
        {
            const string sql =
@"insert into {0} (action, dt, actionType, source, actionGroup, priority)
 values (@action, @dt, @actionType, @source, @actionGroup, @priority)
select @@IDENTITY;";

            using var c = CreateConnection();
            return await c.QuerySingleAsync<int>(sql: string.Format(sql, TableName),
                    param: action);
        }

        public async Task ClearStartedActionAsync()
        {
            const string sql =
@"update {0}
    set duration = null,
        error = null,
        isStarted = 0
    where isStarted = 1
        and isDone = 0";
            using var c = CreateConnection();
            await c.ExecuteAsync(sql: string.Format(sql, TableName), commandTimeout: 60);
        }

        public async Task StartActionAsync(ActionQueue action)
        {
            const string sql =
@"update {0}
    set processDt = getdate(),
        isDone = 0,
        isStarted = 1
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
            await c.ExecuteAsync(sql: string.Format(sql, TableName), action);
        }

        public async Task ClearOldAsync()
        {
            const string sql = "delete from {0} where isDone = 1 and dt < getdate() - 7;";
            using var c = CreateConnection();
            await c.ExecuteAsync(sql: string.Format(sql, TableName));
        }

        public async Task<ActionQueue?> GetActionAsync(int id)
        {
            const string sql = @"select * from {0} where id = @id;";
            using var c = CreateConnection();
            return await c.QuerySingleOrDefaultAsync<ActionQueue>(sql: string.Format(sql, TableName),
                param: new { id });
        }
    }
}