using AdminPanelAPI.Services;
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


// SSH tunnel (optional, you had this)
builder.Services.AddHostedService<SshTunnelService>();

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
