using Npgsql;
using System.Text.Json;

namespace AdminPanelAPI.Services
{
    /// <summary>
    /// Prepares a movie the moment its SF proxy shows up in R2: describes the
    /// film shot by shot and analyses it for key images, both on the proxy and
    /// both without anybody asking, so a tagger who finishes watching finds the
    /// proposals and the walkthrough already there.
    ///
    /// The jobs run on Modal for a quarter of an hour or more, which is why this
    /// lives in the API rather than in the tagging page: nothing has to stay
    /// open for a run to finish or for its results to be stored.
    /// </summary>
    public sealed class MoviePreparationWorker : BackgroundService
    {
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly ILogger<MoviePreparationWorker> _logger;
        private readonly bool _enabled;

        /// <summary>How often R2 is swept for movies with a new SF proxy.</summary>
        private static readonly TimeSpan ScanEvery = TimeSpan.FromMinutes(5);

        /// <summary>How often a job in flight is asked how it is getting on.</summary>
        private static readonly TimeSpan PollEvery = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Give the app a moment to finish starting, and the database tunnel a
        /// moment to come up, before the first sweep.
        /// </summary>
        private static readonly TimeSpan StartupDelay = TimeSpan.FromSeconds(45);

        /// <summary>
        /// How many movies may be in hand at once. Each one is a quarter of an
        /// hour of GPU twice over, so a bucket full of proxies that predate this
        /// is drained a couple of films at a time rather than all at once.
        /// </summary>
        private const int MoviesAtOnce = 2;

        public MoviePreparationWorker(
            IServiceScopeFactory scopeFactory,
            IConfiguration configuration,
            ILogger<MoviePreparationWorker> logger)
        {
            _scopeFactory = scopeFactory;
            _logger = logger;
            _enabled = configuration.GetValue("MovieFiles:AutoPrepare", true);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            if (!_enabled)
            {
                _logger.LogInformation("Automatic movie preparation is switched off.");
                return;
            }

            try
            {
                await Task.Delay(StartupDelay, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            var nextScan = DateTimeOffset.MinValue;

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    if (DateTimeOffset.UtcNow >= nextScan)
                    {
                        await ScanAsync(stoppingToken);
                        nextScan = DateTimeOffset.UtcNow + ScanEvery;
                    }

                    await PollAsync(stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Movie preparation pass failed.");
                }

                try
                {
                    await Task.Delay(PollEvery, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }

        /// <summary>
        /// Every movie folder that holds an SF proxy this has not prepared from,
        /// started off. A movie whose proxy has been re-encoded counts as new,
        /// since the frames and the shot boundaries are the new file's.
        /// </summary>
        private async Task ScanAsync(CancellationToken ct)
        {
            using var scope = _scopeFactory.CreateScope();
            var services = scope.ServiceProvider;
            var connection = services.GetRequiredService<NpgsqlConnection>();
            var storage = services.GetRequiredService<IMovieFileStorageService>();

            await connection.OpenAsync(ct);
            await MoviePreparationStore.EnsureTableAsync(connection, ct);

            var inFlight = (await MoviePreparationStore.ListRunningAsync(connection, ct)).Count;
            if (inFlight >= MoviesAtOnce)
                return;

            var known = await MoviePreparationStore.KnownSourcesAsync(connection, ct);
            var movieIds = await MovieFoldersAsync(storage, ct);
            if (movieIds.Count == 0)
                return;

            var variants = await storage.GetMovieVariantsAsync(movieIds, ct);
            foreach (var movie in variants)
            {
                ct.ThrowIfCancellationRequested();
                if (inFlight >= MoviesAtOnce)
                    return;

                if (movie.SlimKey is not { Length: > 0 } sourceKey)
                    continue;
                if (known.TryGetValue(movie.MovieId, out var prepared) && prepared == sourceKey)
                    continue;

                if (!await MoviePreparationStore.TryClaimAsync(connection, movie.MovieId, sourceKey, ct))
                    continue;

                _logger.LogInformation(
                    "Preparing movie {MovieId} from its SF proxy.", movie.MovieId);
                inFlight += 1;

                await StartWalkthroughAsync(services, connection, movie.MovieId, sourceKey, ct);
                await StartAnalysisAsync(services, connection, movie.MovieId, sourceKey, ct);
            }
        }

        /// <summary>The numeric folders at the top of the bucket: one per movie.</summary>
        private static async Task<List<int>> MovieFoldersAsync(
            IMovieFileStorageService storage, CancellationToken ct)
        {
            var ids = new List<int>();
            string? token = null;

            do
            {
                var page = await storage.ListAsync("", false, token, 1000, ct);
                foreach (var folder in page.Folders)
                {
                    if (int.TryParse(folder.Name, out var movieId) && movieId > 0)
                        ids.Add(movieId);
                }
                token = page.NextToken;
            }
            while (token != null);

            return ids;
        }

        private async Task StartWalkthroughAsync(
            IServiceProvider services,
            NpgsqlConnection connection,
            int movieId,
            string sourceKey,
            CancellationToken ct)
        {
            var walkthrough = services.GetRequiredService<IWalkthroughService>();
            var result = await walkthrough.StartAsync(sourceKey, movieId, ct);

            var (jobId, error) = JobFrom(result);
            await MoviePreparationStore.SetJobAsync(
                connection, movieId, "walkthrough", jobId,
                jobId is null ? MoviePreparationStore.Error : MoviePreparationStore.Running,
                error, ct);
        }

        private async Task StartAnalysisAsync(
            IServiceProvider services,
            NpgsqlConnection connection,
            int movieId,
            string sourceKey,
            CancellationToken ct)
        {
            var analysis = services.GetRequiredService<IKeyImageAnalysisService>();
            var synopsis = services.GetRequiredService<IFilmSynopsisService>();

            var (description, _) = await MovieDescriptions.ForAsync(connection, synopsis, movieId, ct);
            var result = await analysis.StartAsync(sourceKey, movieId, description, ct);

            var (jobId, error) = JobFrom(result);
            await MoviePreparationStore.SetJobAsync(
                connection, movieId, "analysis", jobId,
                jobId is null ? MoviePreparationStore.Error : MoviePreparationStore.Running,
                error, ct);
        }

        /// <summary>
        /// Follow the jobs in flight, and store an analysis's frames as soon as
        /// it finishes — the tagger's grid reads the table, not the job.
        /// </summary>
        private async Task PollAsync(CancellationToken ct)
        {
            using var scope = _scopeFactory.CreateScope();
            var services = scope.ServiceProvider;
            var connection = services.GetRequiredService<NpgsqlConnection>();

            await connection.OpenAsync(ct);
            await MoviePreparationStore.EnsureTableAsync(connection, ct);

            var running = await MoviePreparationStore.ListRunningAsync(connection, ct);
            foreach (var row in running)
            {
                ct.ThrowIfCancellationRequested();

                if (row.WalkthroughStatus == MoviePreparationStore.Running &&
                    row.WalkthroughJobId is { Length: > 0 } walkthroughJob)
                    await PollWalkthroughAsync(services, connection, row.MovieId, walkthroughJob, ct);

                if (row.AnalysisStatus == MoviePreparationStore.Running &&
                    row.AnalysisJobId is { Length: > 0 } analysisJob)
                    await PollAnalysisAsync(services, connection, row.MovieId, analysisJob, ct);
            }
        }

        private async Task PollWalkthroughAsync(
            IServiceProvider services,
            NpgsqlConnection connection,
            int movieId,
            string jobId,
            CancellationToken ct)
        {
            var walkthrough = services.GetRequiredService<IWalkthroughService>();
            var result = await walkthrough.GetJobAsync(jobId, ct);
            if (!result.IsSuccess)
                return;

            using var document = JsonDocument.Parse(result.Body);
            var job = document.RootElement;
            var status = Text(job, "status") ?? MoviePreparationStore.Running;

            await MoviePreparationStore.SetProgressAsync(
                connection, movieId, "walkthrough", status,
                Text(job, "stage"), Number(job, "progress"),
                job.TryGetProperty("shots", out var shots) && shots.ValueKind == JsonValueKind.Number
                    ? shots.GetInt32() : null,
                Text(job, "error"), ct);

            if (status is MoviePreparationStore.Completed or MoviePreparationStore.Error)
                _logger.LogInformation(
                    "Walkthrough for movie {MovieId} {Status}.", movieId, status);
        }

        private async Task PollAnalysisAsync(
            IServiceProvider services,
            NpgsqlConnection connection,
            int movieId,
            string jobId,
            CancellationToken ct)
        {
            var analysis = services.GetRequiredService<IKeyImageAnalysisService>();
            var result = await analysis.GetJobAsync(jobId, includeProposals: true, ct);
            if (!result.IsSuccess)
                return;

            using var document = JsonDocument.Parse(result.Body);
            var job = document.RootElement;
            var status = Text(job, "status") ?? MoviePreparationStore.Running;

            int? proposed = null;
            if (status == MoviePreparationStore.Completed &&
                job.TryGetProperty("proposals", out var proposals) &&
                proposals.ValueKind == JsonValueKind.Array)
            {
                proposed = await KeyImageProposalStore.StoreAsync(connection, movieId, proposals, ct);
                _logger.LogInformation(
                    "Stored {Count} proposals for movie {MovieId} from its automatic analysis.",
                    proposed, movieId);
            }

            await MoviePreparationStore.SetProgressAsync(
                connection, movieId, "analysis", status,
                Text(job, "stage"), Number(job, "progress"), proposed,
                Text(job, "error"), ct);
        }

        /// <summary>A started job's id, or why it could not be started.</summary>
        private static (string? JobId, string? Error) JobFrom(TranscodeResult result)
        {
            if (!result.IsSuccess)
                return (null, Summarize(result.Body));

            try
            {
                using var document = JsonDocument.Parse(result.Body);
                var jobId = Text(document.RootElement, "jobId");
                return jobId is { Length: > 0 }
                    ? (jobId, null)
                    : (null, "The job service returned no job id.");
            }
            catch (JsonException)
            {
                return (null, Summarize(result.Body));
            }
        }

        private static string Summarize(string body)
        {
            try
            {
                using var document = JsonDocument.Parse(body);
                if (Text(document.RootElement, "error") is { Length: > 0 } error)
                    return error;
            }
            catch (JsonException)
            {
                // Not JSON; the body itself is the best explanation there is.
            }

            return body.Length > 400 ? body[..400] : body;
        }

        private static string? Text(JsonElement element, string name) =>
            element.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String
                ? value.GetString()
                : null;

        private static double Number(JsonElement element, string name) =>
            element.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.Number
                ? value.GetDouble()
                : 0;
    }
}
