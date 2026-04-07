using BlobToSharePointMigrator.Models;
using Microsoft.Extensions.Logging;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml.Linq;

namespace BlobToSharePointMigrator.Services;

public class CaseDocumentMetadataService
{
    private static readonly Regex CaseDocumentsFolderRegex = new(@"^(\d+)_Documents$", RegexOptions.IgnoreCase | RegexOptions.Compiled);
    private static readonly Regex CaseManifestRegex = new(@"^case_(\d+)_documents\.xml$", RegexOptions.IgnoreCase | RegexOptions.Compiled);
    private static readonly Regex CollapseWhitespaceRegex = new(@"\s+", RegexOptions.Compiled);
    private readonly ILogger<CaseDocumentMetadataService> _logger;

    public CaseDocumentMetadataService(ILogger<CaseDocumentMetadataService> logger)
    {
        _logger = logger;
    }

    public async Task EnrichAsync(
        IReadOnlyCollection<FileRecord> targetRecords,
        IReadOnlyCollection<FileRecord> allRecords,
        Func<string, Task<Stream>> blobDownloader)
    {
        if (targetRecords.Count == 0)
            return;

        var caseGroups = targetRecords
            .Select(r => new { Record = r, CaseFolderKey = TryGetCaseDocumentsFolderKey(r.BlobPath) })
            .Where(x => !string.IsNullOrWhiteSpace(x.CaseFolderKey))
            .GroupBy(x => x.CaseFolderKey!, StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (caseGroups.Count == 0)
        {
            _logger.LogInformation("No case-document folders detected for metadata enrichment.");
            return;
        }

        var xmlLookupByCaseFolder = BuildXmlLookupByCaseFolder(allRecords);
        var caseIdAssigned = 0;
        var caseTypeAssigned = 0;
        var documentIdAssigned = 0;
        var documentIdUnmatched = 0;
        var manifestMissing = 0;

        foreach (var group in caseGroups)
        {
            var caseFolderKey = group.Key;
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

            if (!xmlLookupByCaseFolder.TryGetValue(caseFolderKey, out var manifestBlobPath) ||
                string.IsNullOrWhiteSpace(manifestBlobPath))
            {
                manifestMissing += recordsInCase.Count;
                _logger.LogWarning("No case manifest XML found for case folder '{CaseFolderKey}'. DocumentId metadata will be skipped for {Count} file(s).",
                    caseFolderKey, recordsInCase.Count);
                continue;
            }

            CaseManifestIndex? manifest;
            try
            {
                await using var xmlStream = await blobDownloader(manifestBlobPath);
                manifest = await CaseManifestIndex.LoadAsync(xmlStream);
            }
            catch (Exception ex)
            {
                manifestMissing += recordsInCase.Count;
                _logger.LogWarning(ex,
                    "Failed to parse case manifest XML '{ManifestBlobPath}'. DocumentId metadata will be skipped for case folder '{CaseFolderKey}'.",
                    manifestBlobPath, caseFolderKey);
                continue;
            }

            foreach (var record in recordsInCase.OrderBy(r => r.BlobPath, StringComparer.OrdinalIgnoreCase))
            {
                if (IsCaseManifestFile(record.Name))
                    continue;

                if (manifest.TryAssignDocumentId(record.Name, out var documentId, out var matchMode))
                {
                    EnsureMetadataDictionary(record)["DocumentId"] = documentId;
                    documentIdAssigned++;
                    _logger.LogDebug("Assigned DocumentId via {MatchMode} match: {BlobPath} -> {DocumentId}",
                        matchMode, record.BlobPath, documentId);
                }
                else
                {
                    documentIdUnmatched++;
                    _logger.LogWarning("No DocumentId match found in '{ManifestBlobPath}' for blob '{BlobPath}' (file name '{FileName}').",
                        manifestBlobPath, record.BlobPath, record.Name);
                }
            }
        }

        _logger.LogInformation(
            "Case metadata enrichment complete: CaseId={CaseIdAssigned}, CaseType={CaseTypeAssigned}, DocumentId={DocumentIdAssigned}, DocumentIdUnmatched={DocumentIdUnmatched}, MissingManifest={MissingManifest}",
            caseIdAssigned,
            caseTypeAssigned,
            documentIdAssigned,
            documentIdUnmatched,
            manifestMissing);
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

    private static Dictionary<string, string> BuildXmlLookupByCaseFolder(IReadOnlyCollection<FileRecord> allRecords)
    {
        var lookup = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        foreach (var record in allRecords)
        {
            if (!IsCaseManifestFile(record.Name))
                continue;

            var caseFolderKey = TryGetCaseDocumentsFolderKey(record.BlobPath);
            if (string.IsNullOrWhiteSpace(caseFolderKey))
                continue;

            var caseNumber = TryExtractCaseNumber(record.BlobPath);
            var match = CaseManifestRegex.Match(record.Name);
            if (!match.Success)
                continue;

            var manifestCaseNumber = match.Groups[1].Value;
            if (!string.Equals(caseNumber, manifestCaseNumber, StringComparison.OrdinalIgnoreCase))
                continue;

            lookup[caseFolderKey] = record.BlobPath;
        }

        return lookup;
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

    private static bool IsCaseManifestFile(string fileName) => CaseManifestRegex.IsMatch(fileName ?? string.Empty);

    private sealed class CaseManifestIndex
    {
        private readonly Dictionary<string, Queue<ManifestDocument>> _exactNameQueues;
        private readonly Dictionary<string, Queue<ManifestDocument>> _normalizedNameQueues;

        private CaseManifestIndex(
            Dictionary<string, Queue<ManifestDocument>> exactNameQueues,
            Dictionary<string, Queue<ManifestDocument>> normalizedNameQueues)
        {
            _exactNameQueues = exactNameQueues;
            _normalizedNameQueues = normalizedNameQueues;
        }

        public static async Task<CaseManifestIndex> LoadAsync(Stream xmlStream)
        {
            var document = await XDocument.LoadAsync(xmlStream, LoadOptions.None, CancellationToken.None);
            var entries = document.Root?
                .Elements("Document")
                .Select((element, index) => new ManifestDocument(
                    id: element.Attribute("Id")?.Value?.Trim() ?? string.Empty,
                    name: element.Attribute("Name")?.Value?.Trim() ?? string.Empty,
                    sequence: index))
                .Where(x => !string.IsNullOrWhiteSpace(x.Id) && !string.IsNullOrWhiteSpace(x.Name))
                .ToList()
                ?? new List<ManifestDocument>();

            var exact = entries
                .GroupBy(x => x.Name, StringComparer.OrdinalIgnoreCase)
                .ToDictionary(
                    g => g.Key,
                    g => new Queue<ManifestDocument>(g.OrderBy(x => x.Sequence)),
                    StringComparer.OrdinalIgnoreCase);

            var normalized = entries
                .GroupBy(x => NormalizeFileNameForMatch(x.Name), StringComparer.OrdinalIgnoreCase)
                .ToDictionary(
                    g => g.Key,
                    g => new Queue<ManifestDocument>(g.OrderBy(x => x.Sequence)),
                    StringComparer.OrdinalIgnoreCase);

            return new CaseManifestIndex(exact, normalized);
        }

        public bool TryAssignDocumentId(string fileName, out string documentId, out string matchMode)
        {
            documentId = string.Empty;
            matchMode = string.Empty;

            if (TryTakeUnassigned(_exactNameQueues, fileName, out var exact))
            {
                exact.Assigned = true;
                documentId = exact.Id;
                matchMode = "exact";
                return true;
            }

            var normalizedName = NormalizeFileNameForMatch(fileName);
            if (TryTakeUnassigned(_normalizedNameQueues, normalizedName, out var normalized))
            {
                normalized.Assigned = true;
                documentId = normalized.Id;
                matchMode = "normalized";
                return true;
            }

            return false;
        }

        private static bool TryTakeUnassigned(
            Dictionary<string, Queue<ManifestDocument>> queues,
            string key,
            out ManifestDocument document)
        {
            document = default!;
            if (!queues.TryGetValue(key, out var queue))
                return false;

            while (queue.Count > 0)
            {
                var candidate = queue.Dequeue();
                if (candidate.Assigned)
                    continue;

                document = candidate;
                return true;
            }

            return false;
        }
    }

    private sealed class ManifestDocument
    {
        public ManifestDocument(string id, string name, int sequence)
        {
            Id = id;
            Name = name;
            Sequence = sequence;
        }

        public string Id { get; }
        public string Name { get; }
        public int Sequence { get; }
        public bool Assigned { get; set; }
    }

    private static string NormalizeFileNameForMatch(string fileName)
    {
        var normalized = (fileName ?? string.Empty).Normalize(NormalizationForm.FormKC);
        normalized = normalized.Replace('\uFFFD', ' ');
        normalized = normalized.Replace('\\', '/');
        normalized = CollapseWhitespaceRegex.Replace(normalized, " ").Trim().TrimEnd('.');
        return normalized.ToLowerInvariant();
    }
}
