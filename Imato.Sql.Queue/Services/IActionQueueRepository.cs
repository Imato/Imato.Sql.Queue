using Imato.Dapper.DbContext;

namespace Imato.Sql.Queue
{
    public interface IActionQueueRepository : IDbContext
    {
        IMyProvider GetProvider();
    }
}