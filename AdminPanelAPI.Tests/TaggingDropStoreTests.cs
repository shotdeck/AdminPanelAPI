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

public class TaggingDropReleaseTests
{
    private static TaggingDropStore.OpenDrop Drop(long id, int day, int titles, int assigned) =>
        new(id, new DateOnly(2026, 10, day), titles, assigned);

    [Fact]
    public void The_first_open_drop_is_always_released()
    {
        var released = TaggingDropStore.Released(new[] { Drop(2, 20, 10, 0), Drop(1, 13, 10, 0) }, 75);
        Assert.Equal(new long[] { 1 }, released.Select(d => d.Id));
    }

    [Fact]
    public void A_later_drop_waits_until_the_earlier_one_is_assigned_enough()
    {
        var drops = new[] { Drop(1, 13, 8, 6), Drop(2, 20, 10, 7), Drop(3, 27, 4, 0) };
        Assert.Equal(new long[] { 1, 2 }, TaggingDropStore.Released(drops, 75).Select(d => d.Id));
        Assert.Equal(new long[] { 1 }, TaggingDropStore.Released(drops, 80).Select(d => d.Id));
        Assert.Equal(new long[] { 1, 2, 3 }, TaggingDropStore.Released(drops, 0).Select(d => d.Id));
    }

    [Fact]
    public void An_empty_drop_does_not_hold_back_the_next()
    {
        var released = TaggingDropStore.Released(new[] { Drop(1, 13, 0, 0), Drop(2, 20, 3, 0) }, 75);
        Assert.Equal(new long[] { 1, 2 }, released.Select(d => d.Id));
    }
}
