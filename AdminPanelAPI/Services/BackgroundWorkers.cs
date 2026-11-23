using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;

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

    /// <summary>
    /// For endpoints that hand a job to an in-process queue: on a slot without
    /// workers nothing would ever pick it up, so refuse before touching any data.
    /// </summary>
    [AttributeUsage(AttributeTargets.Method | AttributeTargets.Class)]
    public sealed class RequiresBackgroundWorkersAttribute : ActionFilterAttribute
    {
        public override void OnActionExecuting(ActionExecutingContext context)
        {
            var configuration = context.HttpContext.RequestServices.GetRequiredService<IConfiguration>();
            if (BackgroundWorkers.Enabled(configuration)) return;

            context.Result = Refusal();
        }

        public static ObjectResult Refusal() => new(new
        {
            error = "Background workers are disabled on this instance; submit jobs to the production API."
        })
        { StatusCode = StatusCodes.Status503ServiceUnavailable };
    }
}
