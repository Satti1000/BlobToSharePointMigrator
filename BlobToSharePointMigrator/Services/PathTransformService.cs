using BlobToSharePointMigrator.Models;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace BlobToSharePointMigrator.Services;

public class PathTransformService
{
    private readonly MappingConfig _mappingConfig;
    private readonly ILogger<PathTransformService> _logger;

    public PathTransformService(string mappingFilePath, ILogger<PathTransformService> logger)
    {
        _logger = logger;
        var json = File.ReadAllText(mappingFilePath);
        _mappingConfig = JsonConvert.DeserializeObject<MappingConfig>(json)
            ?? throw new InvalidOperationException("Failed to load mapping.json");
        _logger.LogInformation("Loaded {Count} folder mappings", _mappingConfig.FolderMappings.Count);
    }

    public string Transform(string blobPath)
    {
        var fileName  = Path.GetFileName(blobPath);
        var directory = blobPath.Contains('/')
            ? blobPath[..blobPath.LastIndexOf('/')]
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

        var mappedPath = $"{bestMatch.Destination}/{fileName}";
        _logger.LogDebug("Mapped: {Source} -> {Destination}", blobPath, mappedPath);
        return mappedPath;
    }

    public List<FileRecord> TransformAll(List<FileRecord> records)
    {
        foreach (var record in records.Where(r => r.IsAllowed))
            record.MappedPath = Transform(record.BlobPath);
        return records;
    }
}
