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
            googleApiKey = "AIzaSyAYLQnLDKYZ-nG11xJE94jt--v3XZB8Xf8";
            if (string.IsNullOrWhiteSpace(googleApiKey))
                return StatusCode(500, "Google Maps API key is missing from configuration.");

            List<AddressCsvRow> rows;

            var csvConfig = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = true,
                MissingFieldFound = null,
                BadDataFound = null,
                TrimOptions = TrimOptions.Trim
            };

            try
            {
                using var stream = request.File.OpenReadStream();
                using var reader = new StreamReader(stream);
                using var csv = new CsvReader(reader, csvConfig);

                csv.Context.RegisterClassMap<AddressCsvRowMap>();
                rows = csv.GetRecords<AddressCsvRow>().ToList();
            }
            catch (Exception ex)
            {
                return BadRequest($"Failed to read CSV: {ex.Message}");
            }

            var httpClient = _httpClientFactory.CreateClient();

            for (int i = 0; i < rows.Count; i++)
            {
                if (i == 5) break;
                cancellationToken.ThrowIfCancellationRequested();

                var row = rows[i];

                if (string.IsNullOrWhiteSpace(row.Clean))
                    continue;

                if (!string.IsNullOrWhiteSpace(row.Latitude) &&
                    !string.IsNullOrWhiteSpace(row.Longitude))
                    continue;

                try
                {
                    var point = await GeocodeAddressAsync(
                        httpClient,
                        row.Clean,
                        googleApiKey,
                        cancellationToken);

                    if (point != null)
                    {
                        row.Latitude = point.Lat.ToString(CultureInfo.InvariantCulture);
                        row.Longitude = point.Lng.ToString(CultureInfo.InvariantCulture);
                    }
                }
                catch
                {
                    // Leave lat/lng blank and continue processing other rows
                }

                await Task.Delay(100, cancellationToken);
            }

            byte[] outputBytes;
            try
            {
                using var memoryStream = new MemoryStream();
                using var writer = new StreamWriter(memoryStream, Encoding.UTF8, leaveOpen: true);
                using var csv = new CsvWriter(writer, csvConfig);

                csv.Context.RegisterClassMap<AddressCsvRowMap>();
                csv.WriteRecords(rows);
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

                throw new Exception($"Google Geocoding API returned status: {geocodeResponse.Status}");
            }

            var first = geocodeResponse.Results?.FirstOrDefault();
            var location = first?.Geometry?.Location;

            if (location == null)
                return null;

            return new GeocodePoint(location.Lat, location.Lng);
        }
    }

    public class AddressCsvRow
    {
        [Name("address")]
        public string? Address { get; set; }

        // Your CSV has a blank second column header
        [Name("")]
        public string? EmptyColumn { get; set; }

        [Name("clean")]
        public string? Clean { get; set; }

        [Name("Latitude")]
        public string? Latitude { get; set; }

        [Name("Longitude")]
        public string? Longitude { get; set; }
    }

    public sealed class AddressCsvRowMap : ClassMap<AddressCsvRow>
    {
        public AddressCsvRowMap()
        {
            Map(m => m.Address).Name("address");
            Map(m => m.EmptyColumn).Name("");
            Map(m => m.Clean).Name("clean");
            Map(m => m.Latitude).Name("Latitude");
            Map(m => m.Longitude).Name("Longitude");
        }
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