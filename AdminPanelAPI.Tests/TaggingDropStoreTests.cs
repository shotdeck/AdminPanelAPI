using AdminPanelAPI.Services;

namespace AdminPanelAPI.Tests;

public class TaggingDropStoreTests
{
    [Fact]
    public void Due_date_defaults_to_the_set_days_before_the_drop()
    {
        Assert.Equal(new DateOnly(2026, 10, 8), TaggingDropStore.DefaultDue(new DateOnly(2026, 10, 13), 5));
        Assert.Equal(new DateOnly(2026, 10, 13), TaggingDropStore.DefaultDue(new DateOnly(2026, 10, 13), -2));
    }

    [Theory]
    [InlineData("2026-10-13", true)]
    [InlineData(" 2026-10-13 ", true)]
    [InlineData("13/10/2026", false)]
    [InlineData("", false)]
    [InlineData(null, false)]
    public void Only_iso_dates_parse(string? value, bool parses)
    {
        Assert.Equal(parses, TaggingDropStore.Parse(value) != null);
    }

    [Fact]
    public void Dates_format_as_iso()
    {
        Assert.Equal("2026-10-08", TaggingDropStore.Format(new DateOnly(2026, 10, 8)));
        Assert.Equal("", TaggingDropStore.Format(null));
    }
}
