using BlobToSharePointMigrator.Models;
using Microsoft.Extensions.Logging;
using System.Text.RegularExpressions;

namespace BlobToSharePointMigrator.Services;

/// <summary>
/// Sets CaseId and CaseType on file records from blob path conventions (e.g. <c>*_Documents</c> folders and case-type path segments).
/// </summary>
public class CaseDocumentMetadataService
{
    private static readonly Regex CaseDocumentsFolderRegex = new(@"^(\d+)_Documents$", RegexOptions.IgnoreCase | RegexOptions.Compiled);
    private readonly ILogger<CaseDocumentMetadataService> _logger;

    public CaseDocumentMetadataService(ILogger<CaseDocumentMetadataService> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// <paramref name="allRecords"/> and <paramref name="blobDownloader"/> are reserved for pipeline shape; path-based enrichment uses only <paramref name="targetRecords"/>.
    /// </summary>
    public Task EnrichAsync(
        IReadOnlyCollection<FileRecord> targetRecords,
        IReadOnlyCollection<FileRecord> allRecords,
        Func<string, Task<Stream>> blobDownloader)
    {
        _ = allRecords;
        _ = blobDownloader;

        if (targetRecords.Count == 0)
            return Task.CompletedTask;

        var caseGroups = targetRecords
            .Select(r => new { Record = r, CaseFolderKey = TryGetCaseDocumentsFolderKey(r.BlobPath) })
            .Where(x => !string.IsNullOrWhiteSpace(x.CaseFolderKey))
            .GroupBy(x => x.CaseFolderKey!, StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (caseGroups.Count == 0)
        {
            _logger.LogInformation("No case-document folders detected for metadata enrichment.");
            return Task.CompletedTask;
        }

        var caseIdAssigned = 0;
        var caseTypeAssigned = 0;

        foreach (var group in caseGroups)
        {
            var recordsInCase = group.Select(x => x.Record).ToList();
            var caseNumber = TryExtractCaseNumber(recordsInCase[0].BlobPath);
            var caseType = TryExtractCaseType(recordsInCase[0].BlobPath);

            foreach (var record in recordsInCase)
            {
                if (string.IsNullOrWhiteSpace(caseNumber))
                    continue;

                EnsureMetadataDictionary(record)["CaseId"] = caseNumber;
                caseIdAssigned++;

                if (!string.IsNullOrWhiteSpace(caseType))
                {
                    record.Metadata["CaseType"] = caseType;
                    caseTypeAssigned++;
                }
            }
        }

        _logger.LogInformation(
            "Case metadata enrichment complete: CaseId={CaseIdAssigned}, CaseType={CaseTypeAssigned}",
            caseIdAssigned,
            caseTypeAssigned);
        return Task.CompletedTask;
    }

    internal static string? TryExtractCaseNumber(string blobPath)
    {
        var segments = blobPath.Split('/', StringSplitOptions.RemoveEmptyEntries);
        foreach (var segment in segments)
        {
            var match = CaseDocumentsFolderRegex.Match(segment);
            if (match.Success)
                return match.Groups[1].Value;
        }

        return null;
    }

    internal static string? TryExtractCaseType(string blobPath)
    {
        var segments = blobPath.Split('/', StringSplitOptions.RemoveEmptyEntries);
        foreach (var segment in segments)
        {
            if (Regex.IsMatch(segment, @"^NRM_Cases_\d+_M(\d{2}|xx)$", RegexOptions.IgnoreCase))
                return "NRM";
            if (Regex.IsMatch(segment, @"^DTN_Cases_\d+_M(\d{2}|xx)$", RegexOptions.IgnoreCase))
                return "DTN";
            if (Regex.IsMatch(segment, @"^Disqualification_\d+_M(\d{2}|xx)$", RegexOptions.IgnoreCase))
                return "Disqualification";
        }

        return null;
    }

    private static IDictionary<string, string> EnsureMetadataDictionary(FileRecord record)
    {
        record.Metadata ??= new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        if (record.Metadata is not Dictionary<string, string> dictionary ||
            dictionary.Comparer != StringComparer.OrdinalIgnoreCase)
        {
            record.Metadata = new Dictionary<string, string>(record.Metadata, StringComparer.OrdinalIgnoreCase);
        }

        return record.Metadata;
    }

    private static string? TryGetCaseDocumentsFolderKey(string blobPath)
    {
        var normalized = (blobPath ?? string.Empty).Replace('\\', '/').Trim('/');
        if (string.IsNullOrWhiteSpace(normalized))
            return null;

        var segments = normalized.Split('/', StringSplitOptions.RemoveEmptyEntries);
        for (var i = 0; i < segments.Length; i++)
        {
            if (CaseDocumentsFolderRegex.IsMatch(segments[i]))
                return string.Join("/", segments.Take(i + 1));
        }

        return null;
    }
}
