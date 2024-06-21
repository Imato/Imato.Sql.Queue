using Microsoft.Extensions.DependencyInjection;

namespace Imato.Sql.Queue
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddActionQueue(this IServiceCollection services,
            Action<QueueSettings>? settingsFactory = null)
        {
            var settings = new QueueSettings();
            settingsFactory?.Invoke(settings);
            services.AddSingleton(settings);
            services.AddSingleton<IActionQueueRepository, ActionQueueRepository>();
            services.AddSingleton<IActionQueueService, ActionQueueService>();
            return services;
        }
    }
}