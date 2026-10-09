using AdminPanelAPI.Bts.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;

namespace AdminPanelAPI.Bts.Services;

public static class AccessKeys
{
    public const string Space = "bts.space";
    public const string Admin = "bts.admin";

    public static string? BearerToken(HttpRequest request)
    {
        var header = request.Headers.Authorization.ToString();
        return header.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase) ? header[7..].Trim() : null;
    }

    public static IActionResult Denied(string message) =>
        new ObjectResult(new ErrorResponse(message)) { StatusCode = StatusCodes.Status401Unauthorized };
}

/// <summary>
/// Customer access: the space link's token, sent as a bearer token, decides
/// which space the request may touch. Nothing the browser sends can change it.
/// </summary>
public sealed class SpaceLinkFilter(SpaceStore store, SpaceCache cache) : IAsyncActionFilter
{
    public async Task OnActionExecutionAsync(ActionExecutingContext context, ActionExecutionDelegate next)
    {
        var token = AccessKeys.BearerToken(context.HttpContext.Request);
        if (!Tokens.LooksLikeSpaceToken(token))
        {
            context.Result = AccessKeys.Denied("This link is not valid.");
            return;
        }

        var hash = Tokens.HashSpaceToken(token!);
        if (!cache.TryGet(hash, out var space))
        {
            space = await store.FindByTokenHashAsync(hash, context.HttpContext.RequestAborted);
            cache.Set(hash, space);
        }

        if (space is null || !space.IsActive)
        {
            context.Result = AccessKeys.Denied("This link is no longer active. Ask ShotDeck for a new one.");
            return;
        }

        context.HttpContext.Items[AccessKeys.Space] = new SpaceContext(space, "customer", false);
        await next();
    }
}

/// <summary>
/// Admin access: a signed session token from <c>/api/admin/login</c>. When the
/// route names a space, it is loaded too, so admin file routes act on it.
/// </summary>
public sealed class AdminFilter(Tokens tokens, SpaceStore store) : IAsyncActionFilter
{
    public async Task OnActionExecutionAsync(ActionExecutingContext context, ActionExecutionDelegate next)
    {
        var name = tokens.ValidateAdmin(AccessKeys.BearerToken(context.HttpContext.Request));
        if (name is null)
        {
            context.Result = AccessKeys.Denied("Please sign in again.");
            return;
        }

        context.HttpContext.Items[AccessKeys.Admin] = name;

        if (context.RouteData.Values.TryGetValue("spaceId", out var raw) && long.TryParse(raw?.ToString(), out var id))
        {
            var space = await store.GetAsync(id, context.HttpContext.RequestAborted);
            if (space is null)
            {
                context.Result = new NotFoundObjectResult(new ErrorResponse("That space does not exist."));
                return;
            }
            context.HttpContext.Items[AccessKeys.Space] = new SpaceContext(space, name, true);
        }

        await next();
    }
}

/// <summary>Turns the services' exceptions into JSON errors with the right status.</summary>
public sealed class ErrorFilter(ILogger<ErrorFilter> logger) : IExceptionFilter
{
    public void OnException(ExceptionContext context)
    {
        var (status, message) = context.Exception switch
        {
            ArgumentException ex => (StatusCodes.Status400BadRequest, ex.Message),
            NotFoundException ex => (StatusCodes.Status404NotFound, ex.Message),
            ConflictException ex => (StatusCodes.Status409Conflict, ex.Message),
            TooLargeException ex => (StatusCodes.Status413PayloadTooLarge, ex.Message),
            Amazon.S3.AmazonS3Exception ex when ex.StatusCode == System.Net.HttpStatusCode.NotFound =>
                (StatusCodes.Status404NotFound, "That item no longer exists."),
            OperationCanceledException => (499, "Request cancelled."),
            _ => (StatusCodes.Status500InternalServerError, "Something went wrong. Please try again.")
        };

        if (status >= 500)
            logger.LogError(context.Exception, "Unhandled error on {Path}", context.HttpContext.Request.Path);

        context.Result = new ObjectResult(new ErrorResponse(message)) { StatusCode = status };
        context.ExceptionHandled = true;
    }
}
