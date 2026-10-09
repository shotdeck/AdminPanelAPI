using AdminPanelAPI.Bts.Services;

namespace AdminPanelAPI.Tests.Bts;

public class PathRulesTests
{
    [Theory]
    [InlineData(null, "")]
    [InlineData("", "")]
    [InlineData("/", "")]
    [InlineData("Day 1", "Day 1/")]
    [InlineData("/Day 1//Stills/", "Day 1/Stills/")]
    public void Folder_normalises(string? input, string expected) =>
        Assert.Equal(expected, PathRules.Folder(input));

    [Theory]
    [InlineData("..")]
    [InlineData("a/../b")]
    [InlineData("a/./b")]
    [InlineData("a\\b")]
    [InlineData("a/\u0000b")]
    public void Folder_rejects_escapes(string input) =>
        Assert.Throws<ArgumentException>(() => PathRules.Folder(input));

    [Fact]
    public void File_requires_a_name() =>
        Assert.Throws<ArgumentException>(() => PathRules.File("Day 1/"));

    [Fact]
    public void File_keeps_folders() =>
        Assert.Equal("Day 1/set.jpg", PathRules.File("/Day 1/set.jpg"));

    [Theory]
    [InlineData("a/b")]
    [InlineData("..")]
    [InlineData("")]
    public void Name_is_a_single_segment(string input) =>
        Assert.Throws<ArgumentException>(() => PathRules.Name(input));

    [Fact]
    public void Too_long_segment_is_rejected() =>
        Assert.Throws<ArgumentException>(() => PathRules.Name(new string('a', PathRules.MaxSegmentLength + 1)));

    [Theory]
    [InlineData("still.JPG", true)]
    [InlineData("take.mov", true)]
    [InlineData("A001.braw", true)]
    [InlineData("notes.pdf", false)]
    [InlineData("payload.html", false)]
    [InlineData("noext", false)]
    public void Only_media_is_allowed(string name, bool allowed) =>
        Assert.Equal(allowed, PathRules.IsAllowedMedia(name));

    [Theory]
    [InlineData("a/b/c.jpg", "a/b/")]
    [InlineData("a/b/", "a/")]
    [InlineData("c.jpg", "")]
    public void ParentOf_works(string input, string expected) =>
        Assert.Equal(expected, PathRules.ParentOf(input));
}
