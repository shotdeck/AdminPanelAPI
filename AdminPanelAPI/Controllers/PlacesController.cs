using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using CsvHelper;
using CsvHelper.Configuration;
using CsvHelper.Configuration.Attributes;
using Microsoft.Extensions.Configuration;
using System.Globalization;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace AdminPanelAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class PlacesController : ControllerBase
    {
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly IConfiguration _configuration;

        public PlacesController(
            IHttpClientFactory httpClientFactory,
            IConfiguration configuration)
        {
            _httpClientFactory = httpClientFactory;
            _configuration = configuration;
        }
        public class GeocodeCsvRequest
        {
            public IFormFile File { get; set; } = default!;
        }

        [HttpPost("process")]
        [Consumes("multipart/form-data")]
        public async Task<IActionResult> ProcessCsv([FromForm] GeocodeCsvRequest request, CancellationToken cancellationToken)
        {
            if (request.File == null || request.File.Length == 0)
                return BadRequest("CSV file is required.");

            var googleApiKey = _configuration["GoogleMaps:ApiKey"];
            if (string.IsNullOrWhiteSpace(googleApiKey))
                return StatusCode(500, "Google Maps API key is missing from configuration.");

            var csvConfig = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = true,
                MissingFieldFound = null,
                BadDataFound = null,
                TrimOptions = TrimOptions.Trim
            };

            List<string> headers;
            List<Dictionary<string, string>> rows;

            try
            {
                using var stream = request.File.OpenReadStream();
                using var reader = new StreamReader(stream);
                using var csv = new CsvReader(reader, csvConfig);

                if (!await csv.ReadAsync() || !csv.ReadHeader())
                    return BadRequest("CSV does not contain a valid header row.");

                headers = csv.HeaderRecord?.ToList() ?? new List<string>();

                if (!headers.Any(h => string.Equals(h, "clean", StringComparison.OrdinalIgnoreCase)))
                    return BadRequest("CSV must contain a 'clean' column.");

                var hasLatitude = headers.Any(h => string.Equals(h, "Latitude", StringComparison.OrdinalIgnoreCase));
                var hasLongitude = headers.Any(h => string.Equals(h, "Longitude", StringComparison.OrdinalIgnoreCase));

                if (!hasLatitude)
                    headers.Add("Latitude");

                if (!hasLongitude)
                    headers.Add("Longitude");

                rows = new List<Dictionary<string, string>>();

                while (await csv.ReadAsync())
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    var row = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

                    foreach (var header in csv.HeaderRecord!)
                    {
                        row[header] = csv.GetField(header) ?? string.Empty;
                    }

                    // Ensure Latitude/Longitude keys exist even if not in original CSV
                    if (!row.ContainsKey("Latitude"))
                        row["Latitude"] = string.Empty;

                    if (!row.ContainsKey("Longitude"))
                        row["Longitude"] = string.Empty;

                    rows.Add(row);
                }
            }
            catch (Exception ex)
            {
                return BadRequest($"Failed to read CSV: {ex.Message}");
            }

            var httpClient = _httpClientFactory.CreateClient();

            // Small in-memory cache so duplicate addresses don't call Google twice
            var geocodeCache = new Dictionary<string, GeocodePoint?>(StringComparer.OrdinalIgnoreCase);

            foreach (var row in rows)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var cleanHeader = headers.First(h => string.Equals(h, "clean", StringComparison.OrdinalIgnoreCase));
                var cleanValue = row.TryGetValue(cleanHeader, out var clean) ? clean?.Trim() : null;

                if (string.IsNullOrWhiteSpace(cleanValue))
                    continue;

                var latHeader = headers.First(h => string.Equals(h, "Latitude", StringComparison.OrdinalIgnoreCase));
                var lngHeader = headers.First(h => string.Equals(h, "Longitude", StringComparison.OrdinalIgnoreCase));

                var existingLat = row.TryGetValue(latHeader, out var lat) ? lat?.Trim() : null;
                var existingLng = row.TryGetValue(lngHeader, out var lng) ? lng?.Trim() : null;

                // If both already populated, leave them alone
                if (!string.IsNullOrWhiteSpace(existingLat) && !string.IsNullOrWhiteSpace(existingLng))
                    continue;

                try
                {
                    if (!geocodeCache.TryGetValue(cleanValue, out var point))
                    {
                        point = await GeocodeAddressAsync(httpClient, cleanValue, googleApiKey, cancellationToken);
                        geocodeCache[cleanValue] = point;

                        // tiny delay to reduce risk of hammering the API
                        await Task.Delay(75, cancellationToken);
                    }

                    if (point != null)
                    {
                        if (string.IsNullOrWhiteSpace(existingLat))
                            row[latHeader] = point.Lat.ToString(CultureInfo.InvariantCulture);

                        if (string.IsNullOrWhiteSpace(existingLng))
                            row[lngHeader] = point.Lng.ToString(CultureInfo.InvariantCulture);
                    }
                }
                catch
                {
                    // Ignore row-level failure and continue
                }
            }

            byte[] outputBytes;

            try
            {
                using var memoryStream = new MemoryStream();
                using var writer = new StreamWriter(memoryStream, new UTF8Encoding(true), leaveOpen: true);
                using var csvWriter = new CsvWriter(writer, csvConfig);

                // Write header in original order, with Latitude/Longitude appended if they were missing
                foreach (var header in headers)
                {
                    csvWriter.WriteField(header);
                }

                await csvWriter.NextRecordAsync();

                // Write rows preserving all original columns plus lat/lng
                foreach (var row in rows)
                {
                    foreach (var header in headers)
                    {
                        row.TryGetValue(header, out var value);
                        csvWriter.WriteField(value ?? string.Empty);
                    }

                    await csvWriter.NextRecordAsync();
                }

                await writer.FlushAsync();
                memoryStream.Position = 0;
                outputBytes = memoryStream.ToArray();
            }
            catch (Exception ex)
            {
                return StatusCode(500, $"Failed to write output CSV: {ex.Message}");
            }

            var outputFileName = Path.GetFileNameWithoutExtension(request.File.FileName) + "-geocoded.csv";
            return File(outputBytes, "text/csv", outputFileName);
        }

        private static async Task<GeocodePoint?> GeocodeAddressAsync(
            HttpClient httpClient,
            string address,
            string googleApiKey,
            CancellationToken cancellationToken)
        {
            var url =
                $"https://maps.googleapis.com/maps/api/geocode/json?address={Uri.EscapeDataString(address)}&key={googleApiKey}";

            using var response = await httpClient.GetAsync(url, cancellationToken);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadAsStringAsync(cancellationToken);

            var geocodeResponse = JsonSerializer.Deserialize<GoogleGeocodeResponse>(json, new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            });

            if (geocodeResponse == null)
                return null;

            if (!string.Equals(geocodeResponse.Status, "OK", StringComparison.OrdinalIgnoreCase))
            {
                if (string.Equals(geocodeResponse.Status, "ZERO_RESULTS", StringComparison.OrdinalIgnoreCase))
                    return null;

                throw new Exception($"Google returned status: {geocodeResponse.Status}");
            }

            var location = geocodeResponse.Results?.FirstOrDefault()?.Geometry?.Location;
            if (location == null)
                return null;

            return new GeocodePoint(location.Lat, location.Lng);
        }
    }

    public class GeocodeCsvRequest
    {
        public IFormFile File { get; set; } = default!;
    }

    public record GeocodePoint(double Lat, double Lng);

    public class GoogleGeocodeResponse
    {
        [JsonPropertyName("status")]
        public string? Status { get; set; }

        [JsonPropertyName("results")]
        public List<GoogleGeocodeResult>? Results { get; set; }
    }

    public class GoogleGeocodeResult
    {
        [JsonPropertyName("geometry")]
        public GoogleGeocodeGeometry? Geometry { get; set; }
    }

    public class GoogleGeocodeGeometry
    {
        [JsonPropertyName("location")]
        public GoogleGeocodeLocation? Location { get; set; }
    }

    public class GoogleGeocodeLocation
    {
        [JsonPropertyName("lat")]
        public double Lat { get; set; }

        [JsonPropertyName("lng")]
        public double Lng { get; set; }
    }
}