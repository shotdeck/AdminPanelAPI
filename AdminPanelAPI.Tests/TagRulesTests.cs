using AdminPanelAPI.Services;
using Xunit;

namespace AdminPanelAPI.Tests;

public class TagRulesTests
{
    private const string Taxonomy = """
    {
      "categories": {
        "time_of_day": { "values": ["day", "night", "dusk"], "legal_combinations": [["day"], ["day", "dusk"]] },
        "lighting_type": { "values": ["daylight", "moonlight", "sunny", "practical_light", "firelight"] },
        "lighting_direction": { "values": ["ambient_light", "backlight"] }
      },
      "constraints": [
        { "category": "lighting_type", "never_together": ["daylight", "moonlight"], "message": "Daylight and Moonlight can't be together." },
        { "category": "lighting_type", "value": "practical_light", "needs_one_of": ["firelight"], "message": "Practical Light needs a source." },
        { "category": "lighting_type", "value": "sunny", "needs_one_of": ["daylight"], "message": "Sunny needs Daylight with it." },
        { "category": "lighting_direction", "value": "ambient_light", "alone": true, "message": "Ambient Light stands on its own." }
      ]
    }
    """;

    private static List<string> Check(string category, string value) =>
        TagRules.Errors(Taxonomy, new Dictionary<string, string> { [category] = value });

    [Theory]
    [InlineData("lighting_type", "daylight + sunny")]
    [InlineData("lighting_type", "practical_light + firelight")]
    [InlineData("lighting_direction", "ambient_light")]
    [InlineData("time_of_day", "day + dusk")]
    [InlineData("time_of_day", "night")]
    [InlineData("lighting_type", "")]
    public void AllowsPossibleCombinations(string category, string value) =>
        Assert.Empty(Check(category, value));

    [Theory]
    [InlineData("lighting_type", "daylight + moonlight", "Daylight and Moonlight can't be together.")]
    [InlineData("lighting_type", "practical_light", "Practical Light needs a source.")]
    [InlineData("lighting_type", "sunny", "Sunny needs Daylight with it.")]
    [InlineData("lighting_direction", "ambient_light + backlight", "Ambient Light stands on its own.")]
    [InlineData("time_of_day", "day + night", "day + night is not a combination time_of_day can take.")]
    public void RefusesImpossibleCombinations(string category, string value, string error) =>
        Assert.Equal(new[] { error }, Check(category, value));

    [Fact]
    public void UnreadableTaxonomyRefusesNothing() =>
        Assert.Empty(TagRules.Errors("not json", new Dictionary<string, string> { ["x"] = "a + b" }));
}
