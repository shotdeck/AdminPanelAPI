using AdminPanelAPI.Models;
using AdminPanelAPI.Services;

namespace AdminPanelAPI.Tests;

public class ClipRelocateTests
{
    private static SceneBoundaryResponse Boundary(double start, double end) => new()
    {
        MovieId = 1,
        Filename = "ABC",
        StartTime = start,
        EndTime = end,
        CutTimes = new List<double> { 1.0, 2.84, 4.55, 8.0 },
        Duration = 9.0,
        Fps = 24.0,
        TargetFrame = 90, // 3.75s
    };

    [Fact]
    public void Short_shot_widens_two_seconds_past_its_cuts()
    {
        var b = Boundary(2.695, 4.695);
        ClipRelocateService.WidenShortShot(b);
        Assert.Equal(0.84, b.StartTime!.Value, 6);
        Assert.Equal(6.55, b.EndTime!.Value, 6);
    }

    [Fact]
    public void Widening_is_clamped_to_the_clip()
    {
        var b = Boundary(0.0, 2.0);
        b.CutTimes = new List<double> { 1.5, 8.5 };
        b.TargetFrame = 12; // 0.5s, before the first cut
        ClipRelocateService.WidenShortShot(b);
        Assert.Equal(0.0, b.StartTime!.Value, 6);
        Assert.Equal(3.5, b.EndTime!.Value, 6);
    }

    [Fact]
    public void Other_lengths_are_left_alone()
    {
        var b = Boundary(2.84, 4.55);
        ClipRelocateService.WidenShortShot(b);
        Assert.Equal(2.84, b.StartTime!.Value, 6);
        Assert.Equal(4.55, b.EndTime!.Value, 6);
    }
}
