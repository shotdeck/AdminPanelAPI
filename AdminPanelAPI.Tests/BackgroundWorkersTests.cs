using AdminPanelAPI.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Abstractions;
using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace AdminPanelAPI.Tests;

public class BackgroundWorkersTests
{
    private static ActionExecutingContext Context(string? enabled)
    {
        var settings = new Dictionary<string, string?>();
        if (enabled != null) settings["BackgroundWorkers:Enabled"] = enabled;
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(settings).Build();
        var http = new DefaultHttpContext
        {
            RequestServices = new ServiceCollection()
                .AddSingleton<IConfiguration>(configuration)
                .BuildServiceProvider()
        };
        var action = new ActionContext(http, new RouteData(), new ActionDescriptor());
        return new ActionExecutingContext(action, new List<IFilterMetadata>(), new Dictionary<string, object?>(), new object());
    }

    [Theory]
    [InlineData(null)]
    [InlineData("true")]
    public void Enabled_instance_lets_job_submissions_through(string? enabled)
    {
        var context = Context(enabled);
        new RequiresBackgroundWorkersAttribute().OnActionExecuting(context);
        Assert.Null(context.Result);
    }

    [Fact]
    public void Disabled_instance_refuses_job_submissions()
    {
        var context = Context("false");
        new RequiresBackgroundWorkersAttribute().OnActionExecuting(context);
        var result = Assert.IsType<ObjectResult>(context.Result);
        Assert.Equal(StatusCodes.Status503ServiceUnavailable, result.StatusCode);
    }

    [Fact]
    public void Disabled_instance_registers_no_workers()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?> { ["BackgroundWorkers:Enabled"] = "false" })
            .Build();
        var services = new ServiceCollection().AddBackgroundWorker<MovieProcessingWorker>(configuration);
        Assert.Empty(services);
    }
}
