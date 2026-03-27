using BlobToSharePointMigrator.Models;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Text.RegularExpressions;

namespace BlobToSharePointMigrator.Services;

public class PathTransformService
{
    private readonly MappingConfig _mappingConfig;
    private readonly bool _useYyyyCaseNumberPath;
    private readonly ILogger<PathTransformService> _logger;

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
                _logger.LogDebug("Mapped (YYYY/CaseNumber): {Source} -> {Destination}", blobPath, yyyyCasePath);
                return yyyyCasePath;
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

        _logger.LogDebug("Mapped: {Source} -> {Destination}", blobPath, mappedPath);
        return mappedPath;
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
}
