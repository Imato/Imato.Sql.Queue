using Imato.Dapper.DbContext;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Imato.Sql.Queue
{
    public class ActionQueueRepository : DbContext, IActionQueueRepository
    {
        private readonly QueueSettings _settings;
        private IMyProvider? _dbProvider;

        public ActionQueueRepository(IConfiguration configuration,
            ILogger<ActionQueueRepository> logger,
            QueueSettings settings)
            : base(configuration, logger)
        {
            _settings = settings;
        }

        public IMyProvider GetProvider()
        {
            var vendor = Vendor(ConnectionString(_settings.ConnectionStringName));
            if (_dbProvider == null)
            {
                switch (vendor)
                {
                    case ContextVendors.mssql:
                        _dbProvider = new MyMsSqlProvider(ConnectionString(_settings.ConnectionStringName));
                        break;

                    case ContextVendors.postgres:
                        _dbProvider = new MyPostgresProvider(ConnectionString(_settings.ConnectionStringName));
                        break;

                    default:
                        throw new NotImplementedException();
                }
            }

            return _dbProvider!;
        }
    }
}