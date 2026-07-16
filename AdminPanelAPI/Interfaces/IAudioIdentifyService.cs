using AdminPanelAPI.Models;

/// <summary>
/// Audio-listening identification fallback: extracts the clip's real audio
/// from the movie and asks an audio LLM what the music is. Used to correct
/// weak/false fingerprint matches (the model "listens" the way ACRCloud/AudD
/// can't reason about). Advisory only — a human confirms before applying.
/// </summary>
public interface IAudioIdentifyService
{
    Task<AudioIdentifySuggestion> IdentifyAsync(
        string r2Key,
        double start,
        double end,
        string currentTitle,
        string? currentArtist,
        string movieTitle,
        int? movieYear,
        IReadOnlyList<string> soundtrackCues,
        CancellationToken cancellationToken);
}
