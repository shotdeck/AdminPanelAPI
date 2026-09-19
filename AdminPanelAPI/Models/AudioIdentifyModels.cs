namespace AdminPanelAPI.Models
{
    /// <summary>
    /// A suggestion from the audio-listening identification fallback (an audio
    /// LLM that "listens" to the clip). Advisory only: it reliably tells whether
    /// the fingerprint is wrong and names the likely composer/kind of music, but
    /// the exact title is often approximate, so a human confirms before it is
    /// applied.
    /// </summary>
    public class AudioIdentifySuggestion
    {
        public string? Title { get; set; }
        public string? Artist { get; set; }
        /// <summary>
        /// True when the title came from the web-search reasoning pass (a
        /// best-guess "possible cue") rather than being recognised by ear, so the
        /// UI can flag it as unverified.
        /// </summary>
        public bool TitleUnverified { get; set; }
        public bool IsScoreCue { get; set; }
        public string? Confidence { get; set; }
        public string? Explanation { get; set; }
        public string? Raw { get; set; }
        public string? Error { get; set; }

        /// <summary>
        /// Ranked shortlist of the most likely soundtrack cues (best first) when
        /// the exact title can't be pinned by ear. Each carries listen links so a
        /// human can play the candidates and pick the right one. Empty when there
        /// is no soundtrack to draw from.
        /// </summary>
        public IReadOnlyList<AudioCueCandidate> Candidates { get; set; }
            = System.Array.Empty<AudioCueCandidate>();
    }

    /// <summary>
    /// One candidate cue in the audio-ID shortlist, with links so the user can
    /// listen and decide which one actually plays in the scene.
    /// </summary>
    public class AudioCueCandidate
    {
        public string Title { get; set; } = "";
        public string? Artist { get; set; }
        public string? SpotifyUrl { get; set; }
        public string? YouTubeUrl { get; set; }
    }

    /// <summary>
    /// A track/cue from a movie's official soundtrack album, with its Spotify
    /// track link when available. Used both as a grounding hint for audio ID and
    /// to build the "pick a cue" shortlist with listen links.
    /// </summary>
    public class SoundtrackCue
    {
        public string Title { get; set; } = "";
        public string? Artist { get; set; }
        public string? SpotifyUrl { get; set; }
    }
}
