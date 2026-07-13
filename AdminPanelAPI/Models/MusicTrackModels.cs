namespace AdminPanelAPI.Models
{
    /// <summary>
    /// A single occurrence of a song inside a movie (one stored segment),
    /// with enough info to play it back from R2.
    /// </summary>
    public class MusicTrackOccurrence
    {
        public int MovieId { get; set; }
        public string? MovieTitle { get; set; }
        public int? MovieYear { get; set; }
        public double StartTime { get; set; }
        public double EndTime { get; set; }
        public double? Score { get; set; }
        public string? Source { get; set; }
        public string? Confidence { get; set; }
    }

    /// <summary>Basic movie metadata used for soundtrack reconciliation.</summary>
    public class MovieInfo
    {
        public int MovieId { get; set; }
        public string? Title { get; set; }
        public int? Year { get; set; }
        public string? PosterUrl { get; set; }
    }

    /// <summary>A distinct identified song in a movie (for reconciliation).</summary>
    public class MovieSongRow
    {
        public long SongId { get; set; }
        public string? Title { get; set; }
        public string? Artist { get; set; }
        public string? Isrc { get; set; }
        public string? SpotifyUrl { get; set; }
        public string? StreamingUrl { get; set; }
        public string? ArtworkUrl { get; set; }
    }

    /// <summary>A movie's official soundtrack album (for display at movie level).</summary>
    public class MovieSoundtrack
    {
        public int MovieId { get; set; }
        public string? AlbumName { get; set; }
        public string? SpotifyUrl { get; set; }
        public string? ArtworkUrl { get; set; }
        public string? WikipediaUrl { get; set; }
    }

    /// <summary>
    /// A credited person on a track (writer / composer / producer). Carries the
    /// MusicBrainz artist id so a future "more tracks by this person" link can
    /// resolve by id rather than by name.
    /// </summary>
    public class MusicCredit
    {
        public string Name { get; set; } = "";
        public string? Mbid { get; set; }
    }

    /// <summary>One playback segment file's index and real start offset (seconds).</summary>
    public class VideoSegment
    {
        public int Index { get; set; }
        public double Start { get; set; }
    }

    /// <summary>A titled external link (e.g. a web citation for a description).</summary>
    public class LinkRef
    {
        public string? Title { get; set; }
        public string Url { get; set; } = "";
    }

    /// <summary>An AI-generated, web-grounded description with its citations.</summary>
    public class AiDescription
    {
        public string? Description { get; set; }
        public List<LinkRef> Sources { get; set; } = new();

        /// <summary>
        /// True when the description was manually edited by an admin. A locked
        /// (edited) description is never overwritten by AI regeneration.
        /// </summary>
        public bool Edited { get; set; }
    }

    /// <summary>Request body for saving a manual (locked) track description.</summary>
    public class SaveDescriptionRequest
    {
        public int MovieId { get; set; }
        public string? Description { get; set; }
    }

    /// <summary>Request body for setting a track's confidence status in a movie.</summary>
    public class SetStatusRequest
    {
        public int MovieId { get; set; }
        public string? Status { get; set; }
    }

    /// <summary>
    /// Enrichment for a single identified track (description + credits +
    /// release metadata), cached after the first lookup.
    /// </summary>
    public class TrackDetails
    {
        public long SongId { get; set; }
        public string? Title { get; set; }
        public string? Artist { get; set; }
        public string? Description { get; set; }
        public string? DescriptionSource { get; set; }
        public List<LinkRef> DescriptionSources { get; set; } = new();
        /// <summary>True when the shown description was manually edited/locked.</summary>
        public bool DescriptionEdited { get; set; }
        /// <summary>
        /// The factual Wikipedia blurb about the track, kept separately so the UI
        /// can show it alongside the (film-specific) AI description in
        /// <see cref="Description"/>, which otherwise overrides it.
        /// </summary>
        public string? WikipediaDescription { get; set; }
        /// <summary>
        /// Set when film-specific AI description generation was attempted but
        /// failed (e.g. OpenAI quota exceeded). Transient — not persisted.
        /// </summary>
        public string? AiDescriptionError { get; set; }
        /// <summary>
        /// The web-search agent's verdict on whether this track actually
        /// appears in the film: "in_film", "not_in_film", or "unclear".
        /// Transient — used to flag likely false-positive matches for review.
        /// </summary>
        public string? AiInFilm { get; set; }
        /// <summary>
        /// True when the track's original release year is later than the film's
        /// release year — i.e. the song didn't exist yet, so the match is almost
        /// certainly a false positive. Transient; only a flagging tie-breaker
        /// when the AI verdict is not a positive "in_film".
        /// </summary>
        public bool ReleasedAfterMovie { get; set; }
        /// <summary>The film's release year, when a movie is in scope.</summary>
        public int? MovieYear { get; set; }
        /// <summary>
        /// The song's original (debut) release year as determined by the
        /// web-search agent. More reliable than the matched recording's date,
        /// which is often a later compilation/remaster. Transient.
        /// </summary>
        public int? OriginalReleaseYear { get; set; }
        public string? WikipediaUrl { get; set; }
        public List<MusicCredit> Writers { get; set; } = new();
        public List<MusicCredit> Composers { get; set; } = new();
        public List<MusicCredit> Producers { get; set; } = new();
        public string? Album { get; set; }
        public string? ReleaseDate { get; set; }
        public string? Label { get; set; }
        public string? PreviewUrl { get; set; }
        public string? SpotifyUrl { get; set; }
        public string? MusicbrainzUrl { get; set; }
    }

    /// <summary>
    /// A movie that has identified music, with how many distinct songs and
    /// occurrences were found in it.
    /// </summary>
    public class MovieMusicSummary
    {
        public int MovieId { get; set; }
        public string? Title { get; set; }
        public int? Year { get; set; }
        public int TrackCount { get; set; }
        public int OccurrenceCount { get; set; }
        public string? PosterUrl { get; set; }
    }

    /// <summary>
    /// The distinct artists and song titles that have identified music, used to
    /// populate the Band/Song search dropdown.
    /// </summary>
    public class MusicSearchOptions
    {
        public List<string> Artists { get; set; } = new();
        public List<string> Songs { get; set; } = new();
    }

    /// <summary>
    /// A song grouped with every occurrence of it (across one or many movies).
    /// </summary>
    public class MusicTrackGroup
    {
        public long SongId { get; set; }
        public string? Title { get; set; }
        public string? Artist { get; set; }
        public string? Isrc { get; set; }
        public string? Acrid { get; set; }
        public string? SpotifyUrl { get; set; }
        public string? StreamingUrl { get; set; }
        public string? ArtworkUrl { get; set; }
        public string? Confidence { get; set; }
        public int OccurrenceCount { get; set; }
        public List<MusicTrackOccurrence> Occurrences { get; set; } = new();
    }
}
