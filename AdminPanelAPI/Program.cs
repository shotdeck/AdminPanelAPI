using Npgsql;
using ShotDeck.Keywords;


var builder = WebApplication.CreateBuilder(args);

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

// Database migrations at startup
builder.Services.AddHostedService<ShotDeckSearch.Services.DatabaseMigrationService>();

// Keyword warmup at startup (singleton, creates scope manually)
builder.Services.AddHostedService<KeywordWarmupService>();

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
