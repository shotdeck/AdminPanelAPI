using AdminPanelAPI.Interfaces;
using AdminPanelAPI.Services;
using Microsoft.AspNetCore.Diagnostics;
using Npgsql;
using ShotDeck.Keywords;


var builder = WebApplication.CreateBuilder(args);

// Allow large file uploads (up to 10 GB for movie files)
builder.WebHost.ConfigureKestrel(options =>
{
    options.Limits.MaxRequestBodySize = 10L * 1024 * 1024 * 1024;
});

// Controllers & Swagger
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();


builder.Services.AddApplicationInsightsTelemetry(options =>
{
    // Azure injects this automatically if you enabled App Insights in the Portal
    options.ConnectionString = builder.Configuration["APPLICATIONINSIGHTS_CONNECTION_STRING"];
});


// Move the tunnel off a loopback port another slot on this worker already
// owns, keeping ConnectionStrings:Default in step. Must run before anything
// reads the connection string.
TunnelPortSelector.Apply(builder.Configuration);

// SSH tunnel (optional, you had this)
// Singleton so the health endpoint can report the tunnel's live state.
builder.Services.AddSingleton<SshTunnelService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<SshTunnelService>());

// Database connection (scoped, lazy - only opened when first accessed)
builder.Services.AddScoped<Lazy<NpgsqlConnection>>(sp =>
{
    return new Lazy<NpgsqlConnection>(() =>
    {
        var connStr = builder.Configuration["ConnectionStrings:Default"]
            ?? throw new InvalidOperationException("DefaultConnection is not configured.");

        var conn = new NpgsqlConnection(connStr);
        conn.Open();
        return conn;
    });
});

// Keep NpgsqlConnection resolvable for code that injects it directly
builder.Services.AddScoped<NpgsqlConnection>(sp => sp.GetRequiredService<Lazy<NpgsqlConnection>>().Value);

// Keyword caching (singleton) - also includes unwanted words caching
builder.Services.AddSingleton<IKeywordCacheService, KeywordCacheService>();

builder.Services.AddHttpClient();
builder.Services.AddHttpClient("HighConcurrency")
    .ConfigurePrimaryHttpMessageHandler(() => new SocketsHttpHandler
    {
        MaxConnectionsPerServer = 100,
        PooledConnectionLifetime = TimeSpan.FromMinutes(10),
    });
builder.Services.AddSingleton<IMovieJobQueue, MovieJobQueue>();

builder.Services.AddScoped<IMovieProcessingJobRepository, MovieProcessingJobRepository>();
builder.Services.AddScoped<IClipPreviewService, ClipPreviewService>();

// Singleton so one start call can keep cutting previews in the background while
// the status endpoint reports on it.
builder.Services.AddSingleton<IClipPreviewMotionRunner, ClipPreviewMotionRunner>();

builder.Services.AddScoped<IMovieProcessingService, MovieProcessingService>();

builder.Services.AddHostedService<MovieProcessingWorker>();

// Caption embedding batch processing
builder.Services.AddSingleton<ICaptionEmbeddingJobQueue, CaptionEmbeddingJobQueue>();
builder.Services.AddScoped<ICaptionEmbeddingJobRepository, CaptionEmbeddingJobRepository>();
builder.Services.AddScoped<ICaptionEmbeddingService, CaptionEmbeddingService>();
builder.Services.AddHostedService<CaptionEmbeddingWorker>();


// Dialogue search transcription pipeline
builder.Services.AddSingleton<IDialogueJobQueue, DialogueJobQueue>();
builder.Services.AddScoped<IDialogueTranscriptionJobRepository, DialogueTranscriptionJobRepository>();
builder.Services.AddScoped<IDialogueTranscriptionService, DialogueTranscriptionService>();
builder.Services.AddHostedService<DialogueTranscriptionWorker>();

// Music identification pipeline (music detection + ACRCloud)
builder.Services.AddSingleton<IMusicJobQueue, MusicJobQueue>();
builder.Services.AddScoped<IMusicIdentificationJobRepository, MusicIdentificationJobRepository>();
builder.Services.AddScoped<IMusicIdentificationService, MusicIdentificationService>();
builder.Services.AddHttpClient<ISoundtrackReconciliationService, SoundtrackReconciliationService>();
builder.Services.AddHttpClient<IStreamingLinkService, StreamingLinkService>();
builder.Services.AddHttpClient<ITrackDetailsService, TrackDetailsService>();
builder.Services.AddScoped<IAudioIdentifyService, AudioIdentifyService>();
builder.Services.AddHostedService<MusicIdentificationWorker>();

// Movie source files in R2 (dashboard file browser)
builder.Services.AddSingleton<IMovieFileStorageService, MovieFileStorageService>();
builder.Services.AddSingleton<IMovieTranscodeService, MovieTranscodeService>();
builder.Services.AddSingleton<IKeyImageAnalysisService, KeyImageAnalysisService>();
builder.Services.AddSingleton<IFilmSynopsisService, FilmSynopsisService>();
builder.Services.AddSingleton<IWalkthroughService, WalkthroughService>();
builder.Services.AddSingleton<IStoryRatingService, StoryRatingService>();
builder.Services.AddSingleton<IImageTechnicalTagService, ImageTechnicalTagService>();

// TMDB lookups for saying which film an uploaded master is
builder.Services.AddSingleton<ITmdbService, TmdbService>();

// Describes and analyses a movie as soon as its SF proxy exists, so a tagger
// who finishes watching finds the walkthrough and the proposals waiting.
builder.Services.AddHostedService<MoviePreparationWorker>();

// Reads the technical terms off the frames a tagger has kept, so they are there
// when the tagger opens an image rather than fetched while they wait.
builder.Services.AddHostedService<KeyImageTagWorker>();

// Cuts the picture of a frame picked while watching just after it is picked,
// rather than making the tagger wait on the cut as they pick.
builder.Services.AddScoped<IKeyImageStillService, KeyImageStillService>();
builder.Services.AddHostedService<KeyImageStillWorker>();

// Camera-movement QC: shared analysis pipeline plus a worker that keeps a bank
// of pre-analysed images so a reviewer's fetch is an instant assignment.
builder.Services.AddScoped<CameraMovementAnalysisService>();
builder.Services.AddHostedService<CameraMovementBankWorker>();
builder.Services.AddHostedService<CameraMovementMovieFetchWorker>();

// Keyword warmup at startup (singleton, creates scope manually)
builder.Services.AddHostedService<KeywordWarmupService>();

// Background geocoding service
builder.Services.AddHostedService<GeocodeBackgroundService>();

// Background movie location populate service
builder.Services.AddHostedService<MovieLocationBackgroundService>();

builder.Services.AddCors(opt =>
{
    opt.AddPolicy("AllowAll", p =>
        p.AllowAnyOrigin()
         .AllowAnyHeader()
         .AllowAnyMethod());
});

var app = builder.Build();

app.UseCors("AllowAll");

/* An unhandled exception resets the response, taking the CORS headers with it,
 * so the browser sees a failed request rather than a 500 and the page can only
 * say the server did not answer. Writing the error here keeps the headers and
 * gives the page something to show. */
app.UseExceptionHandler(errors => errors.Run(async context =>
{
    context.Response.StatusCode = StatusCodes.Status500InternalServerError;
    context.Response.ContentType = "application/json";
    context.Response.Headers["Access-Control-Allow-Origin"] = "*";
    /* What went wrong is logged rather than answered: the API takes any
     * origin, so a database or storage message would be readable by anyone. */
    var failure = context.Features.Get<IExceptionHandlerFeature>()?.Error;
    if (failure != null)
        context.RequestServices
            .GetRequiredService<ILoggerFactory>()
            .CreateLogger("UnhandledError")
            .LogError(failure, "{Method} {Path} failed.",
                context.Request.Method, context.Request.Path);

    await context.Response.WriteAsJsonAsync(new
    {
        error = "The request failed. Please try again."
    });
}));

app.UseStaticFiles();
app.UseSwagger();

app.UseSwaggerUI(c =>
{
    c.RoutePrefix = "swagger"; // <-- final route
    c.SwaggerEndpoint("/swagger/v1/swagger.json", "AdminPanel API v1");
    c.DocumentTitle = "AdminPanel API Docs";
});



app.UseHttpsRedirection();
app.UseAuthorization();
app.MapControllers();
app.Run();
