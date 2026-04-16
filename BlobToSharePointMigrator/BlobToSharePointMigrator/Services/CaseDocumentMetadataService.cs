using BlobToSharePointMigrator.Configuration;
using BlobToSharePointMigrator.Models;
using Microsoft.Extensions.Logging;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml.Linq;

namespace BlobToSharePointMigrator.Services;

/// <summary>
/// Sets CaseId and CaseType from paths; when <see cref="MigrationSettings.AssignDocumentIdFromCaseXml"/> is true,
/// assigns DocumentId per file from <c>case_{caseNumber}_documents.xml</c> manifests (aligned with feature/pro-customized).
/// </summary>
public class CaseDocumentMetadataService
{
    private static readonly Regex CaseDocumentsFolderRegex = new(@"^(\d+)_Documents$", RegexOptions.IgnoreCase | RegexOptions.Compiled);
    private static readonly Regex CaseManifestRegex = new(@"^case_(\d+)_documents\.xml$", RegexOptions.IgnoreCase | RegexOptions.Compiled);
    private static readonly Regex CollapseWhitespaceRegex = new(@"\s+", RegexOptions.Compiled);
    private const int UnmatchedSampleLimit = 5;
    private readonly MigrationSettings _settings;
    private readonly ILogger<CaseDocumentMetadataService> _logger;

    public CaseDocumentMetadataService(MigrationSettings settings, ILogger<CaseDocumentMetadataService> logger)
    {
        _settings = settings;
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

        var xmlLookupByCaseFolder = _settings.AssignDocumentIdFromCaseXml
            ? BuildXmlLookupByCaseFolder(allRecords)
            : null;

        var caseIdAssigned = 0;
        var caseTypeAssigned = 0;
        var documentIdAssigned = 0;
        var documentIdUnmatched = 0;
        var manifestMissing = 0;
        var folderDocumentIdAssigned = 0;

        foreach (var group in caseGroups)
        {
            var caseFolderKey = group.Key;
            var recordsInCase = group.Select(x => x.Record).ToList();
            var caseNumber = TryExtractCaseNumber(recordsInCase[0].BlobPath);
            var caseType = TryExtractCaseType(recordsInCase[0].BlobPath);
            var unmatchedInCase = 0;
            var unmatchedSamples = new List<string>();

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

            if (!_settings.AssignDocumentIdFromCaseXml || xmlLookupByCaseFolder is null)
                continue;

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

                if (manifest.TryAssignDocumentId(record.Name, record.ContentType, out var documentId, out var matchMode))
                {
                    EnsureMetadataDictionary(record)["DocumentId"] = documentId;
                    documentIdAssigned++;
                    _logger.LogDebug("Assigned DocumentId via {MatchMode} match: {BlobPath} -> {DocumentId}",
                        matchMode, record.BlobPath, documentId);
                }
                else
                {
                    documentIdUnmatched++;
                    unmatchedInCase++;
                    if (unmatchedSamples.Count < UnmatchedSampleLimit)
                    {
                        unmatchedSamples.Add($"{record.Name} [{record.ContentType}]");
                    }
                }

                foreach (var folderMatch in manifest.FindFolderDocumentIds(record.MappedPath))
                {
                    if (TryAssignFolderDocumentId(record, folderMatch.RelativeFolderPath, folderMatch.DocumentId))
                    {
                        folderDocumentIdAssigned++;
                        _logger.LogDebug(
                            "Assigned folder DocumentId from manifest: {BlobPath} -> {FolderPath} ({DocumentId})",
                            record.BlobPath,
                            folderMatch.RelativeFolderPath,
                            folderMatch.DocumentId);
                    }
                }
            }

            if (unmatchedInCase > 0)
            {
                _logger.LogWarning(
                    "No DocumentId match found for {Count} file(s) in '{ManifestBlobPath}' for case folder '{CaseFolderKey}'. Continuing without DocumentId. Sample file(s): {Samples}",
                    unmatchedInCase,
                    manifestBlobPath,
                    caseFolderKey,
                    string.Join(", ", unmatchedSamples));
            }
        }

        if (!_settings.AssignDocumentIdFromCaseXml)
        {
            _logger.LogInformation(
                "Case metadata enrichment complete: CaseId={CaseIdAssigned}, CaseType={CaseTypeAssigned}; DocumentId from manifest disabled (AssignDocumentIdFromCaseXml=false).",
                caseIdAssigned,
                caseTypeAssigned);
            return;
        }

        _logger.LogInformation(
            "Case metadata enrichment complete: CaseId={CaseIdAssigned}, CaseType={CaseTypeAssigned}, DocumentId={DocumentIdAssigned}, DocumentIdUnmatched={DocumentIdUnmatched}, MissingManifest={MissingManifest}",
            caseIdAssigned,
            caseTypeAssigned,
            documentIdAssigned,
            documentIdUnmatched,
            manifestMissing);
        if (folderDocumentIdAssigned > 0)
        {
            _logger.LogInformation("Folder-level DocumentId assignments: {FolderDocumentIdAssigned}", folderDocumentIdAssigned);
        }
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

    private static bool TryAssignFolderDocumentId(FileRecord record, string relativeFolderPath, string documentId)
    {
        if (string.IsNullOrWhiteSpace(relativeFolderPath) || string.IsNullOrWhiteSpace(documentId))
            return false;

        var normalizedRelativeFolderPath = NormalizeRelativePathForMatch(relativeFolderPath);
        if (string.IsNullOrWhiteSpace(normalizedRelativeFolderPath))
            return false;

        record.FolderMetadata ??= new Dictionary<string, IDictionary<string, string>>(StringComparer.OrdinalIgnoreCase);
        if (!record.FolderMetadata.TryGetValue(normalizedRelativeFolderPath, out var metadata))
        {
            metadata = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            record.FolderMetadata[normalizedRelativeFolderPath] = metadata;
        }

        if (metadata.TryGetValue("DocumentId", out var existing) &&
            string.Equals(existing, documentId, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        metadata["DocumentId"] = documentId;
        return true;
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
        private readonly Dictionary<string, List<ManifestDocument>> _exactNameLookup;
        private readonly Dictionary<string, List<ManifestDocument>> _normalizedNameLookup;
        private readonly Dictionary<string, List<ManifestDocument>> _typedNameLookup;
        private readonly Dictionary<string, List<ManifestDocument>> _stemNameLookup;
        private readonly List<ManifestFolderDocument> _folderDocuments;

        private CaseManifestIndex(
            Dictionary<string, List<ManifestDocument>> exactNameLookup,
            Dictionary<string, List<ManifestDocument>> normalizedNameLookup,
            Dictionary<string, List<ManifestDocument>> typedNameLookup,
            Dictionary<string, List<ManifestDocument>> stemNameLookup,
            List<ManifestFolderDocument> folderDocuments)
        {
            _exactNameLookup = exactNameLookup;
            _normalizedNameLookup = normalizedNameLookup;
            _typedNameLookup = typedNameLookup;
            _stemNameLookup = stemNameLookup;
            _folderDocuments = folderDocuments;
        }

        public static async Task<CaseManifestIndex> LoadAsync(Stream xmlStream)
        {
            var document = await XDocument.LoadAsync(xmlStream, LoadOptions.None, CancellationToken.None);
            var entries = document.Root?
                .Elements("Document")
                .Select((element, index) => new ManifestDocument(
                    id: element.Attribute("Id")?.Value?.Trim() ?? string.Empty,
                    name: element.Attribute("Name")?.Value?.Trim() ?? string.Empty,
                    type: element.Attribute("Type")?.Value?.Trim() ?? string.Empty,
                    category: element.Attribute("Category")?.Value?.Trim() ?? string.Empty,
                    source: element.Attribute("Source")?.Value?.Trim() ?? string.Empty,
                    sequence: index))
                .Where(x => !string.IsNullOrWhiteSpace(x.Id) && !string.IsNullOrWhiteSpace(x.Name))
                .ToList()
                ?? new List<ManifestDocument>();

            var folderEntries = entries
                .Where(IsFolderLikeDocument)
                .ToList();

            var fileEntries = entries
                .Where(x => !IsFolderLikeDocument(x))
                .ToList();

            var folderDocuments = folderEntries
                .Select(CreateFolderDocument)
                .Where(x => x != null)
                .Cast<ManifestFolderDocument>()
                .OrderBy(x => x.Sequence)
                .ToList();

            var exact = fileEntries
                .GroupBy(x => x.Name, StringComparer.OrdinalIgnoreCase)
                .ToDictionary(
                    g => g.Key,
                    g => g.OrderBy(x => x.Sequence).ToList(),
                    StringComparer.OrdinalIgnoreCase);

            var normalized = fileEntries
                .GroupBy(x => x.NormalizedName, StringComparer.OrdinalIgnoreCase)
                .ToDictionary(
                    g => g.Key,
                    g => g.OrderBy(x => x.Sequence).ToList(),
                    StringComparer.OrdinalIgnoreCase);

            var typed = fileEntries
                .SelectMany(x => x.ExpectedTypedNames.Select(name => new { Name = name, Document = x }))
                .GroupBy(x => x.Name, StringComparer.OrdinalIgnoreCase)
                .ToDictionary(
                    g => g.Key,
                    g => g.Select(x => x.Document).OrderBy(x => x.Sequence).ToList(),
                    StringComparer.OrdinalIgnoreCase);

            // Stem lookup keys use each entry's NormalizedName (XML names have no file extension). Blob-side
            // matching uses NormalizeFileStemForMatch on the filename so e.g. XML "Acknowledgement letter v2.0"
            // matches blob "Acknowledgement letter v2.0.doc".
            var stems = fileEntries
                .GroupBy(x => x.NormalizedName, StringComparer.OrdinalIgnoreCase)
                .ToDictionary(
                    g => g.Key,
                    g => g.OrderBy(x => x.Sequence).ToList(),
                    StringComparer.OrdinalIgnoreCase);

            return new CaseManifestIndex(exact, normalized, typed, stems, folderDocuments);
        }

        public bool TryAssignDocumentId(string fileName, string? contentType, out string documentId, out string matchMode)
        {
            documentId = string.Empty;
            matchMode = string.Empty;

            if (TryTakeUnassigned(_exactNameLookup, fileName, out var exact))
            {
                exact.Assigned = true;
                documentId = exact.Id;
                matchMode = "exact";
                return true;
            }

            var normalizedName = NormalizeFileNameForMatch(fileName);
            if (TryTakeUnassigned(_normalizedNameLookup, normalizedName, out var normalized))
            {
                normalized.Assigned = true;
                documentId = normalized.Id;
                matchMode = "normalized";
                return true;
            }

            if (TryTakeUnassigned(_typedNameLookup, normalizedName, out var typed))
            {
                typed.Assigned = true;
                documentId = typed.Id;
                matchMode = "name+type";
                return true;
            }

            var normalizedStem = NormalizeFileStemForMatch(fileName);
            if (!string.IsNullOrWhiteSpace(normalizedStem) &&
                TryTakeUnassigned(_stemNameLookup, normalizedStem, out var stem))
            {
                stem.Assigned = true;
                documentId = stem.Id;
                matchMode = "extensionless";
                return true;
            }

            return false;
        }

        public IEnumerable<ManifestFolderDocument> FindFolderDocumentIds(string? mappedPath)
        {
            if (string.IsNullOrWhiteSpace(mappedPath) || _folderDocuments.Count == 0)
                return Array.Empty<ManifestFolderDocument>();

            var normalizedPath = NormalizeRelativePathForMatch(mappedPath);
            if (string.IsNullOrWhiteSpace(normalizedPath))
                return Array.Empty<ManifestFolderDocument>();

            return _folderDocuments.Where(folder =>
                normalizedPath.Contains("/" + folder.RelativeFolderPath + "/", StringComparison.OrdinalIgnoreCase) ||
                normalizedPath.EndsWith("/" + folder.RelativeFolderPath, StringComparison.OrdinalIgnoreCase) ||
                string.Equals(normalizedPath, folder.RelativeFolderPath, StringComparison.OrdinalIgnoreCase));
        }

        private static bool TryTakeUnassigned(
            Dictionary<string, List<ManifestDocument>> lookups,
            string key,
            out ManifestDocument document)
        {
            document = default!;
            if (!lookups.TryGetValue(key, out var candidates))
                return false;

            foreach (var candidate in candidates)
            {
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
        public ManifestDocument(string id, string name, string type, string category, string source, int sequence)
        {
            Id = id;
            Name = name;
            Type = type;
            Category = category;
            Source = source;
            Sequence = sequence;
            NormalizedName = NormalizeFileNameForMatch(name);
            NormalizedStem = NormalizeFileStemForMatch(name);
            ExpectedTypedNames = BuildExpectedTypedNames(name, type);
        }

        public string Id { get; }
        public string Name { get; }
        public string Type { get; }
        public string Category { get; }
        public string Source { get; }
        public int Sequence { get; }
        public string NormalizedName { get; }
        public string NormalizedStem { get; }
        public IReadOnlyCollection<string> ExpectedTypedNames { get; }
        public bool Assigned { get; set; }
    }

    private sealed class ManifestFolderDocument
    {
        public ManifestFolderDocument(string relativeFolderPath, string documentId, int sequence)
        {
            RelativeFolderPath = relativeFolderPath;
            DocumentId = documentId;
            Sequence = sequence;
        }

        public string RelativeFolderPath { get; }
        public string DocumentId { get; }
        public int Sequence { get; }
    }

    private static string NormalizeFileNameForMatch(string fileName)
    {
        var normalized = (fileName ?? string.Empty).Normalize(NormalizationForm.FormKC);
        normalized = normalized.Replace('\uFFFD', ' ');
        normalized = normalized.Replace('\\', '/');
        normalized = CollapseWhitespaceRegex.Replace(normalized, " ").Trim().TrimEnd('.');
        return normalized.ToLowerInvariant();
    }

    private static string NormalizeFileStemForMatch(string fileName)
    {
        var normalizedName = NormalizeFileNameForMatch(fileName);
        var extension = Path.GetExtension(normalizedName);

        // Only strip if the extension looks like a real file extension.
        // Purely numeric suffixes like .20, .2, .0 are version numbers, not extensions,
        // and must not be stripped (e.g. "document v.20" -> stem should stay "document v.20").
        if (string.IsNullOrWhiteSpace(extension) || !IsRealFileExtension(extension))
            return normalizedName;

        return normalizedName[..^extension.Length].TrimEnd('.');
    }

    private static bool IsRealFileExtension(string extension)
    {
        // Expect: starts with '.', followed by 1-6 alphanumeric chars, NOT purely digits.
        var ext = extension.TrimStart('.');
        if (ext.Length == 0 || ext.Length > 6)
            return false;
        if (!ext.All(char.IsLetterOrDigit))
            return false;
        // Purely numeric = version number (.20, .2, .0 ...), not a file extension.
        return !ext.All(char.IsDigit);
    }

    private static IReadOnlyCollection<string> BuildExpectedTypedNames(string name, string type)
    {
        var candidates = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var normalizedName = NormalizeFileNameForMatch(name);
        if (!string.IsNullOrWhiteSpace(normalizedName))
            candidates.Add(normalizedName);

        var baseName = NormalizeFileStemForMatch(name);
        if (string.IsNullOrWhiteSpace(baseName))
            return candidates.ToList();

        foreach (var extension in GetExtensionsForMimeType(type))
            candidates.Add($"{baseName}{extension}");

        return candidates.ToList();
    }

    private static bool IsFolderLikeDocument(ManifestDocument document)
    {
        if (!string.Equals(document.Source, "Document", StringComparison.OrdinalIgnoreCase))
            return false;

        return string.Equals(document.Category, "Email", StringComparison.OrdinalIgnoreCase) ||
               string.Equals(document.Category, "Incoming email", StringComparison.OrdinalIgnoreCase) ||
               string.Equals(document.Category, "Incoming Email", StringComparison.OrdinalIgnoreCase);
    }

    private static ManifestFolderDocument? CreateFolderDocument(ManifestDocument document)
    {
        var folderName = NormalizeFolderNameForMatch(document.Name);
        if (string.IsNullOrWhiteSpace(folderName))
            return null;

        return new ManifestFolderDocument(folderName, document.Id, document.Sequence);
    }

    private static string NormalizeFolderNameForMatch(string? folderName)
    {
        var normalized = NormalizeFileNameForMatch(folderName ?? string.Empty);
        return normalized.Replace("/", "_").Trim('_', '.', ' ');
    }

    private static string NormalizeRelativePathForMatch(string? path)
    {
        var normalized = (path ?? string.Empty).Replace('\\', '/').Trim('/');
        if (string.IsNullOrWhiteSpace(normalized))
            return string.Empty;

        var segments = normalized
            .Split('/', StringSplitOptions.RemoveEmptyEntries)
            .Select(NormalizeFolderNameForMatch)
            .Where(s => !string.IsNullOrWhiteSpace(s));
        return string.Join("/", segments);
    }

    private static IEnumerable<string> GetExtensionsForMimeType(string? type)
    {
        var normalizedType = NormalizeMimeType(type);
        return normalizedType switch
        {
            "application/msword" => new[] { ".doc" },
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document" => new[] { ".docx" },
            "application/pdf" => new[] { ".pdf" },
            "application/vnd.ms-outlook" => new[] { ".msg" },
            "message/rfc822" => new[] { ".eml" },
            "application/vnd.ms-excel" => new[] { ".xls" },
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" => new[] { ".xlsx" },
            "application/vnd.ms-powerpoint" => new[] { ".ppt" },
            "application/vnd.openxmlformats-officedocument.presentationml.presentation" => new[] { ".pptx" },
            "image/jpeg" => new[] { ".jpg", ".jpeg" },
            "image/png" => new[] { ".png" },
            "image/tiff" => new[] { ".tif", ".tiff" },
            "text/plain" => new[] { ".txt" },
            "text/rtf" => new[] { ".rtf" },
            _ => Array.Empty<string>()
        };
    }

    private static string NormalizeMimeType(string? type)
    {
        if (string.IsNullOrWhiteSpace(type))
            return string.Empty;

        var semicolonIndex = type.IndexOf(';');
        var clean = semicolonIndex >= 0 ? type[..semicolonIndex] : type;
        return clean.Trim().ToLowerInvariant();
    }
}
