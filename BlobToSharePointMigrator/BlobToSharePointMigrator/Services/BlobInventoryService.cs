using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using BlobToSharePointMigrator.Configuration;
using BlobToSharePointMigrator.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Text.RegularExpressions;

namespace BlobToSharePointMigrator.Services;

public class BlobInventoryService
{
    private readonly MigrationSettings _settings;
    private IConfigurationSection _processFlags;
    private readonly ILogger<BlobInventoryService> _logger;
    private readonly BlobServiceClient _blobServiceClient;

    public BlobInventoryService(IConfigurationSection processFlags, MigrationSettings settings, ILogger<BlobInventoryService> logger)
    {
        _settings = settings;
        _logger = logger;
        _blobServiceClient = new BlobServiceClient(settings.BlobConnectionString);
        _processFlags = processFlags;
    }

    public async Task<List<FileRecord>> InventoryAsync()
    {
        _logger.LogInformation("Connecting to Azure Blob Storage...");
        var containerClient = _blobServiceClient.GetBlobContainerClient(_settings.SourceContainer);

        _logger.LogInformation("Inventorying container: {Container}", _settings.SourceContainer);

        var records = new List<FileRecord>();
        var allowedExtensions = new HashSet<string>(_settings.AllowedExtensions, StringComparer.OrdinalIgnoreCase);
        var requiredCountByYearAndCaseType = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        var blobFolderPrefix = _processFlags.GetSection("BlobFolderPrefix").Value;
        var prefix = blobFolderPrefix?.TrimEnd('/') ?? "";
        var basePrefix = string.IsNullOrEmpty(prefix) ? string.Empty : prefix + "/";

        var candidatePrefixes = await BuildTargetPrefixesAsync(containerClient, basePrefix);
        if (candidatePrefixes.Count == 0)
        {
            _logger.LogWarning("No targeted prefixes found under base prefix '{BasePrefix}'. Falling back to full-prefix scan.", basePrefix);
            candidatePrefixes.Add(basePrefix);
        }

        _logger.LogInformation("Scanning {PrefixCount} targeted prefixes for migration candidates.", candidatePrefixes.Count);

        foreach (var candidatePrefix in candidatePrefixes)
        {
            await foreach (BlobItem blob in containerClient.GetBlobsAsync(prefix: candidatePrefix))
            {
                if (!IsUnderCaseNumberDocuments(blob.Name))
                    continue;

                var ext = Path.GetExtension(blob.Name).ToLowerInvariant();
                var allowed = allowedExtensions.Contains(ext);
                var skipReason = allowed ? string.Empty : $"Extension '{ext}' not in allowed list";

                var record = new FileRecord
                {
                    Name = Path.GetFileName(blob.Name),
                    BlobPath = blob.Name,
                    SizeBytes = blob.Properties.ContentLength ?? 0,
                    ContentType = blob.Properties.ContentType ?? "application/octet-stream",
                    LastModified = blob.Properties.LastModified?.ToString("yyyy-MM-dd HH:mm:ss") ?? string.Empty,
                    CreatedOn = blob.Properties.CreatedOn?.ToString("yyyy-MM-dd HH:mm:ss") ?? string.Empty,
                    IsAllowed = allowed,
                    SkipReason = skipReason,
                    Metadata = blob.Metadata ?? new Dictionary<string, string>()
                };

                records.Add(record);
                if (allowed)
                {
                    var groupKey = TryGetYearCaseTypeKey(blob.Name);
                    if (!string.IsNullOrWhiteSpace(groupKey))
                    {
                        requiredCountByYearAndCaseType.TryGetValue(groupKey, out var current);
                        requiredCountByYearAndCaseType[groupKey] = current + 1;
                    }
                }
            }
        }

        _logger.LogInformation("Inventory complete. Total: {Total}, Allowed: {Allowed}, Skipped: {Skipped}",
            records.Count,
            records.Count(r => r.IsAllowed),
            records.Count(r => !r.IsAllowed));

        if (requiredCountByYearAndCaseType.Count > 0)
        {
            _logger.LogInformation("Required count by Year/CaseType:");
            foreach (var kv in requiredCountByYearAndCaseType
                .OrderBy(k => k.Key, StringComparer.OrdinalIgnoreCase))
            {
                _logger.LogInformation("  {Group}: {Count}", kv.Key, kv.Value);
            }
        }

        return records;
    }

    public async Task<Stream> DownloadBlobAsync(string blobPath)
    {
        var containerClient = _blobServiceClient.GetBlobContainerClient(_settings.SourceContainer);
        var blobClient = containerClient.GetBlobClient(blobPath);
        var response = await blobClient.DownloadStreamingAsync();
        return response.Value.Content;
    }

    private static string? TryGetYearCaseTypeKey(string blobPath)
    {
        var yearMatch = Regex.Match(blobPath, @"(?<!\d)(20\d{2})(?!\d)", RegexOptions.IgnoreCase);
        if (!yearMatch.Success)
            return null;

        var caseTypeMatch = Regex.Match(blobPath, @"\b(NRM_Cases|DTN_Cases|Disqualification)_", RegexOptions.IgnoreCase);
        if (!caseTypeMatch.Success)
            return null;

        return $"{yearMatch.Groups[1].Value}/{caseTypeMatch.Groups[1].Value}";
    }

    private static bool IsUnderCaseNumberDocuments(string blobPath)
    {
        return Regex.IsMatch(blobPath, @"\b\d+_Documents\/", RegexOptions.IgnoreCase);
    }

    private static async Task<List<string>> BuildTargetPrefixesAsync(BlobContainerClient containerClient, string basePrefix)
    {
        var yearFolders = await GetDirectSubfoldersAsync(containerClient, basePrefix);
        var years = yearFolders
            .Where(y => Regex.IsMatch(y, @"^\d{4}$"))
            .OrderBy(y => y, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var prefixes = new List<string>();
        var caseFamilies = new[] { "NRM_Cases_", "DTN_Cases_", "Disqualification_" };

        foreach (var year in years)
        {
            for (int month = 1; month <= 12; month++)
            {
                var monthToken = $"M{month:00}";
                var yearMonthPrefix = $"{basePrefix}{year}/{monthToken}/";
                foreach (var family in caseFamilies)
                    prefixes.Add($"{yearMonthPrefix}{family}");
            }
        }

        return prefixes;
    }

    private static async Task<List<string>> GetDirectSubfoldersAsync(BlobContainerClient containerClient, string prefix)
    {
        var subfolders = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        await foreach (var item in containerClient.GetBlobsByHierarchyAsync(prefix: prefix, delimiter: "/"))
        {
            if (!item.IsPrefix || string.IsNullOrWhiteSpace(item.Prefix))
                continue;

            var remainder = item.Prefix[prefix.Length..].Trim('/');
            if (!string.IsNullOrWhiteSpace(remainder) && !remainder.Contains('/'))
                subfolders.Add(remainder);
        }
        return subfolders.ToList();
    }
}
