using System.Text.Json;

namespace AdminPanelAPI.Services;

/// <summary>
/// The image tagger's rules on which terms can go together: each category's
/// legal combinations, and the combinations that can never be right (Daylight
/// with Moonlight, Practical Light on its own...). A save that breaks them is refused.
/// </summary>
public static class TagRules
{
    public static List<string> Errors(string taxonomyJson, IReadOnlyDictionary<string, string> chosen)
    {
        var errors = new List<string>();
        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(taxonomyJson);
        }
        catch (JsonException)
        {
            return errors;
        }

        using (document)
        {
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object) return errors;

            if (root.TryGetProperty("categories", out var categories) &&
                categories.ValueKind == JsonValueKind.Object)
            {
                var byName = new Dictionary<string, JsonElement>(StringComparer.OrdinalIgnoreCase);
                foreach (var property in categories.EnumerateObject()) byName[property.Name] = property.Value;

                foreach (var (name, value) in chosen)
                {
                    var terms = Terms(value);
                    if (terms.Count < 2 ||
                        !byName.TryGetValue(name, out var category) ||
                        category.ValueKind != JsonValueKind.Object ||
                        !category.TryGetProperty("legal_combinations", out var combos) ||
                        combos.ValueKind != JsonValueKind.Array)
                        continue;

                    var legal = combos.EnumerateArray().Any(combo =>
                        combo.ValueKind == JsonValueKind.Array &&
                        Strings(combo).SetEquals(terms));
                    if (!legal)
                        errors.Add($"{string.Join(" + ", terms)} is not a combination {name} can take.");
                }
            }

            if (root.TryGetProperty("constraints", out var constraints) &&
                constraints.ValueKind == JsonValueKind.Array)
            {
                foreach (var rule in constraints.EnumerateArray())
                {
                    if (rule.ValueKind != JsonValueKind.Object ||
                        !rule.TryGetProperty("category", out var category) ||
                        category.GetString() is not { } name)
                        continue;

                    // Category names are matched without regard to case, as the save's
                    // legal-values check does; every spelling the save carries is checked.
                    foreach (var (chosenName, value) in chosen)
                    {
                        if (!string.Equals(chosenName, name, StringComparison.OrdinalIgnoreCase))
                            continue;

                        var terms = Terms(value);
                        bool broken;
                        if (rule.TryGetProperty("never_together", out var together))
                        {
                            broken = Strings(together).All(terms.Contains);
                        }
                        else
                        {
                            var term = rule.TryGetProperty("value", out var v) ? v.GetString() : null;
                            broken = term != null && terms.Contains(term) && (
                                (rule.TryGetProperty("alone", out var alone) &&
                                 alone.ValueKind == JsonValueKind.True && terms.Count > 1) ||
                                (rule.TryGetProperty("needs_one_of", out var needs) &&
                                 !Strings(needs).Any(terms.Contains)));
                        }

                        if (broken)
                            errors.Add(rule.TryGetProperty("message", out var message) &&
                                       message.GetString() is { Length: > 0 } text
                                ? text
                                : $"That combination of {name} can't be right.");
                    }
                }
            }
        }

        return errors;
    }

    private static HashSet<string> Terms(string value) =>
        value.Split('+', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

    private static HashSet<string> Strings(JsonElement array) =>
        array.ValueKind != JsonValueKind.Array
            ? new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            : array.EnumerateArray()
                .Select(e => e.GetString())
                .OfType<string>()
                .ToHashSet(StringComparer.OrdinalIgnoreCase);
}
