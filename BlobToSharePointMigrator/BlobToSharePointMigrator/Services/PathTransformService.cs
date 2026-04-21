using BlobToSharePointMigrator.Models;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Text.RegularExpressions;

namespace BlobToSharePointMigrator.Services;

public class PathTransformService
{
    private readonly MappingConfig _mappingConfig;
    private readonly bool _useYyyyCaseNumberPath;
    private readonly string _blobFolderPrefix;
    private readonly string _sharePointTargetFolder;
    // Future: dynamic ETL rules switchable via config. For now, behaves like default mapping.
    private readonly ILogger<PathTransformService> _logger;
    private static readonly Regex InvalidSharePointChars = new Regex(@"[~""#%&\*\{\}\\:<>\?/+\|]", RegexOptions.Compiled);

    public PathTransformService(
        string mappingFilePath,
        bool useYyyyCaseNumberPath,
        ILogger<PathTransformService> logger,
        string? blobFolderPrefix = null,
        string? sharePointTargetFolder = null)
    {
        _logger = logger;
        _useYyyyCaseNumberPath = useYyyyCaseNumberPath;
        _blobFolderPrefix = (blobFolderPrefix ?? string.Empty).Replace('\\', '/').Trim('/');
        _sharePointTargetFolder = (sharePointTargetFolder ?? string.Empty).Replace('\\', '/').Trim('/');
        var json = File.ReadAllText(mappingFilePath);
        _mappingConfig = JsonConvert.DeserializeObject<MappingConfig>(json)
            ?? throw new InvalidOperationException("Failed to load mapping.json");
        _logger.LogInformation("Loaded {Count} folder mappings", _mappingConfig.FolderMappings.Count);
    }

    public string Transform(string blobPath)
    {
        var normalizedBlobPath = blobPath.Replace('\\', '/').Trim('/');

        if (!string.IsNullOrWhiteSpace(_blobFolderPrefix) || !string.IsNullOrWhiteSpace(_sharePointTargetFolder))
        {
            // Keep this branch aligned with DynamicETL's resolver so the same
            // source path + config produces the same SharePoint-relative path.
            var candidate = ResolveSharePointPath(
                normalizedBlobPath,
                _blobFolderPrefix,
                _sharePointTargetFolder,
                _useYyyyCaseNumberPath);
            var safeDyn = SanitizeSharePointRelativePath(candidate);
            _logger.LogDebug("Mapped (Data Migration): {Source} -> {Destination}", blobPath, safeDyn);
            return safeDyn;
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
        // First blob to claim a destination wins (by sorted blob path for stable runs); later collisions are skipped
        // — no __dup* renames in SharePoint.
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var record in records.Where(r => r.IsAllowed).OrderBy(r => r.BlobPath, StringComparer.OrdinalIgnoreCase))
        {
            var mapped = Transform(record.BlobPath);
            if (seen.Contains(mapped))
            {
                record.IsAllowed = false;
                record.SkipReason =
                    $"Duplicate mapped path: same destination as another file ({mapped}). Skipped so only one file uses that name in SharePoint.";
                _logger.LogWarning(
                    "Duplicate mapped path: skipping source \"{Blob}\" — destination \"{Mapped}\" is already claimed by another file.",
                    record.BlobPath,
                    mapped);
                continue;
            }

            seen.Add(mapped);
            record.MappedPath = mapped;
        }
        return records;
    }

    private static string ResolveSharePointPath(string blobName, string prefix, string sharePointTargetFolder, bool useYyyyCaseNumberPath)
    {
        string relativePath;
        if (string.IsNullOrEmpty(prefix))
        {
            relativePath = blobName;
        }
        else
        {
            relativePath = blobName.StartsWith(prefix + "/", StringComparison.OrdinalIgnoreCase)
                ? blobName[(prefix.Length + 1)..]
                : blobName;
        }

        if (useYyyyCaseNumberPath)
        {
            var transformed = TransformToYyyyCaseNumberPath(blobName);
            if (transformed != null)
                return transformed;
        }

        if (!string.IsNullOrEmpty(sharePointTargetFolder))
        {
            if (relativePath.StartsWith(sharePointTargetFolder + "/", StringComparison.OrdinalIgnoreCase))
                return relativePath;
            return $"{sharePointTargetFolder}/{relativePath}";
        }

        return string.IsNullOrEmpty(prefix) ? blobName : $"{prefix}/{relativePath}";
    }

    private static string? TransformToYyyyCaseNumberPath(string blobPath)
    {
        var segments = CaseDocumentsPathRules.SplitPathSegments(blobPath);
        if (segments.Length < 3) return null;

        string? year = segments[2];
        var documentsIndex = CaseDocumentsPathRules.FindAlignedDocumentsSegmentIndex(segments);
        string? caseNumber = null;
        if (documentsIndex >= 0)
        {
            var docsMatch = Regex.Match(segments[documentsIndex], @"^(\d+)_Documents$", RegexOptions.IgnoreCase);
            if (docsMatch.Success)
                caseNumber = docsMatch.Groups[1].Value;
        }

        if (year == null || caseNumber == null || documentsIndex < 0 || documentsIndex >= segments.Length - 1)
            return null;

        var rest = string.Join("/", segments[(documentsIndex + 1)..]);
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

    internal static string TruncateSharePointRelativePath(string path, int maxTotalLength = 400)
    {
        var normalized = (path ?? string.Empty).Replace('\\', '/').Trim('/');
        if (string.IsNullOrWhiteSpace(normalized))
            return string.Empty;
        if (normalized.Length <= maxTotalLength)
            return normalized;

        var segments = normalized.Split('/', StringSplitOptions.RemoveEmptyEntries).ToArray();
        if (segments.Length == 0)
            return normalized;

        // Preserve extension on final segment while truncating.
        var fileName = segments[^1];
        var ext = Path.GetExtension(fileName);
        var baseName = fileName[..Math.Max(0, fileName.Length - ext.Length)];
        var hash = Math.Abs(StringComparer.OrdinalIgnoreCase.GetHashCode(normalized)).ToString("X");

        // First pass: cap each non-final segment more aggressively.
        for (var i = 0; i < segments.Length - 1; i++)
        {
            if (segments[i].Length > 64)
                segments[i] = segments[i][..64];
        }

        // Build compact final file segment.
        var maxFileBase = Math.Max(16, 120 - (ext.Length + hash.Length + 1));
        if (baseName.Length > maxFileBase)
            baseName = baseName[..maxFileBase];
        segments[^1] = $"{baseName}-{hash}{ext}";

        var candidate = string.Join("/", segments);
        if (candidate.Length <= maxTotalLength)
            return candidate;

        // Second pass: trim path segments from the right (excluding first two: YYYY/case when present).
        for (var i = segments.Length - 2; i >= 0 && candidate.Length > maxTotalLength; i--)
        {
            var floor = i <= 1 ? 8 : 3; // keep higher fidelity for year/case
            if (segments[i].Length > floor)
            {
                var cutBy = Math.Min(segments[i].Length - floor, candidate.Length - maxTotalLength);
                segments[i] = segments[i][..(segments[i].Length - cutBy)];
                candidate = string.Join("/", segments);
            }
        }

        // Last-resort hard cut (still keeps end of filename hash/ext).
        if (candidate.Length > maxTotalLength)
            candidate = candidate[..maxTotalLength].Trim('/');

        return candidate;
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
