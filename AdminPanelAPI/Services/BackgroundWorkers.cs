namespace AdminPanelAPI.Services
{
    /// <summary>
    /// Job workers claim work from the shared database, so only one App Service
    /// slot may run them. After a swap the staging slot keeps running the
    /// previous build, so set <c>BackgroundWorkers__Enabled=false</c> on staging
    /// as a deployment slot setting.
    /// </summary>
    public static class BackgroundWorkers
    {
        public static bool Enabled(IConfiguration configuration) =>
            configuration.GetValue("BackgroundWorkers:Enabled", true);

        public static IServiceCollection AddBackgroundWorker<T>(
            this IServiceCollection services, IConfiguration configuration)
            where T : class, IHostedService
        {
            if (Enabled(configuration))
                services.AddHostedService<T>();
            return services;
        }
    }
}
