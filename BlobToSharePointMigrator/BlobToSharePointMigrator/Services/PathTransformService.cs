using BlobToSharePointMigrator.Models;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Text.RegularExpressions;

namespace BlobToSharePointMigrator.Services;

public class PathTransformService
{
    private readonly MappingConfig _mappingConfig;
    private readonly bool _useYyyyCaseNumberPath;
    // Future: dynamic ETL rules switchable via config. For now, behaves like default mapping.
    private readonly ILogger<PathTransformService> _logger;
    private static readonly Regex InvalidSharePointChars = new Regex(@"[~""#%&\*\{\}\\:<>\?/+\|]", RegexOptions.Compiled);

    public PathTransformService(string mappingFilePath, bool useYyyyCaseNumberPath, ILogger<PathTransformService> logger)
    {
        _logger = logger;
        _useYyyyCaseNumberPath = useYyyyCaseNumberPath;
        var json = File.ReadAllText(mappingFilePath);
        _mappingConfig = JsonConvert.DeserializeObject<MappingConfig>(json)
            ?? throw new InvalidOperationException("Failed to load mapping.json");
        _logger.LogInformation("Loaded {Count} folder mappings", _mappingConfig.FolderMappings.Count);
    }

    public string Transform(string blobPath)
    {
        var normalizedBlobPath = blobPath.Replace('\\', '/').Trim('/');

        if (_useYyyyCaseNumberPath)
        {
            var yyyyCasePath = TransformToYyyyCaseNumberPath(normalizedBlobPath);
            if (!string.IsNullOrWhiteSpace(yyyyCasePath))
            {
                var safe = SanitizeSharePointRelativePath(yyyyCasePath);
                _logger.LogDebug("Mapped (YYYY/CaseNumber): {Source} -> {Destination}", blobPath, safe);
                return safe;
            }
        }

        var fileName  = Path.GetFileName(normalizedBlobPath);
        var directory = normalizedBlobPath.Contains('/')
            ? normalizedBlobPath[..normalizedBlobPath.LastIndexOf('/')]
            : string.Empty;

        // Find best matching mapping (longest match wins)
        FolderMapping? bestMatch = null;
        foreach (var mapping in _mappingConfig.FolderMappings)
        {
            if (directory.StartsWith(mapping.Source, StringComparison.OrdinalIgnoreCase))
            {
                if (bestMatch == null || mapping.Source.Length > bestMatch.Source.Length)
                    bestMatch = mapping;
            }
        }

        // Fall back to default (empty source = General)
        bestMatch ??= _mappingConfig.FolderMappings.FirstOrDefault(m => m.Source == string.Empty)
            ?? new FolderMapping { Source = string.Empty, Destination = "General" };

        var sourcePrefix = (bestMatch.Source ?? string.Empty).Replace('\\', '/').Trim('/');
        var destinationRoot = (bestMatch.Destination ?? "General").Replace('\\', '/').Trim('/');

        // Preserve relative subfolders under the matched source to avoid filename collisions
        // when many files share the same basename (e.g. case_details_*.xml).
        var relativeDir = directory;
        if (!string.IsNullOrEmpty(sourcePrefix) &&
            relativeDir.StartsWith(sourcePrefix, StringComparison.OrdinalIgnoreCase))
        {
            relativeDir = relativeDir[sourcePrefix.Length..].Trim('/');
        }

        var mappedPath = string.IsNullOrEmpty(relativeDir)
            ? $"{destinationRoot}/{fileName}"
            : $"{destinationRoot}/{relativeDir}/{fileName}";

        var safeMapped = SanitizeSharePointRelativePath(mappedPath);
        _logger.LogDebug("Mapped: {Source} -> {Destination}", blobPath, safeMapped);
        return safeMapped;
    }

    public List<FileRecord> TransformAll(List<FileRecord> records)
    {
        var seen = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        foreach (var record in records.Where(r => r.IsAllowed))
        {
            var mapped = Transform(record.BlobPath);
            if (seen.TryGetValue(mapped, out var count))
            {
                count++;
                seen[mapped] = count;
                mapped = AppendDuplicateSuffix(mapped, count);
                _logger.LogWarning("Duplicate mapped path detected. Adjusted to: {MappedPath}", mapped);
            }
            else
            {
                seen[mapped] = 0;
            }

            record.MappedPath = mapped;
        }
        return records;
    }

    private static string AppendDuplicateSuffix(string mappedPath, int duplicateIndex)
    {
        var ext = Path.GetExtension(mappedPath);
        var withoutExt = mappedPath[..^ext.Length];
        return $"{withoutExt}__dup{duplicateIndex}{ext}";
    }

    private static string? TransformToYyyyCaseNumberPath(string blobPath)
    {
        var segments = blobPath.Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (segments.Length < 3)
            return null;

        var year = segments[2];
        if (!Regex.IsMatch(year, @"^\d{4}$"))
            return null;

        string? caseNumber = null;
        var documentsIndex = -1;
        for (int i = 0; i < segments.Length; i++)
        {
            var match = Regex.Match(segments[i], @"^(\d+)_Documents$", RegexOptions.IgnoreCase);
            if (match.Success)
            {
                caseNumber = match.Groups[1].Value;
                documentsIndex = i;
                break;
            }
        }

        if (documentsIndex < 0 || string.IsNullOrWhiteSpace(caseNumber))
            return null;

        var rest = documentsIndex + 1 < segments.Length
            ? string.Join("/", segments[(documentsIndex + 1)..])
            : string.Empty;

        return string.IsNullOrEmpty(rest)
            ? $"{year}/{caseNumber}"
            : $"{year}/{caseNumber}/{rest}";
    }
    
    internal static string SanitizeSharePointRelativePath(string path)
    {
        var normalized = (path ?? string.Empty).Replace('\\', '/').Trim('/');
        if (string.IsNullOrWhiteSpace(normalized))
            return string.Empty;

        var segments = normalized.Split('/', StringSplitOptions.RemoveEmptyEntries);
        var safeSegments = new List<string>(segments.Length);

        foreach (var seg in segments)
        {
            var s = seg.Trim();

            // Replace invalid characters (SharePoint/URL + SPMI strictness)
            s = InvalidSharePointChars.Replace(s, "_");

            // SharePoint disallows segments ending in dot, and often rejects trailing spaces too.
            s = s.Trim().TrimEnd('.');

            // Collapse whitespace
            s = Regex.Replace(s, @"\s+", " ").Trim();

            if (string.IsNullOrWhiteSpace(s))
                s = "_";

            // Best-effort cap; long segments can fail in import with opaque errors.
            const int maxSegmentLength = 128;
            if (s.Length > maxSegmentLength)
            {
                // Keep start, add short hash to reduce collisions
                var hash = Math.Abs(StringComparer.OrdinalIgnoreCase.GetHashCode(s)).ToString("X");
                s = s[..Math.Max(1, maxSegmentLength - (hash.Length + 1))] + "-" + hash;
            }

            safeSegments.Add(s);
        }

        return string.Join("/", safeSegments);
    }

    internal static bool ContainsInvalidSharePointChars(string path)
    {
        var normalized = (path ?? string.Empty).Replace('\\', '/').Trim('/');
        if (string.IsNullOrWhiteSpace(normalized))
            return false;

        // Validate per-segment so path separators do not trigger false positives.
        var segments = normalized.Split('/', StringSplitOptions.RemoveEmptyEntries);
        foreach (var segment in segments)
        {
            if (InvalidSharePointChars.IsMatch(segment))
                return true;
        }

        return false;
    }
}
