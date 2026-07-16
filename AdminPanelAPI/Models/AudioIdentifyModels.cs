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
    }
}
