using BlobToSharePointMigrator.Configuration;
using BlobToSharePointMigrator.Models;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Globalization;
using CsvHelper;

namespace BlobToSharePointMigrator.Services;

public class ReportService
{
    private readonly MigrationSettings _settings;
    private readonly ILogger<ReportService> _logger;
    private Dictionary<string, string> _deltaTracking = new();

    public ReportService(MigrationSettings settings, ILogger<ReportService> logger)
    {
        _settings = settings;
        _logger   = logger;
    }

    public void LoadDeltaTracking()
    {
        if (!_settings.DeltaMode || !File.Exists(_settings.DeltaTrackingFile))
            return;

        var json = File.ReadAllText(_settings.DeltaTrackingFile);
        _deltaTracking = JsonConvert.DeserializeObject<Dictionary<string, string>>(json)
            ?? new Dictionary<string, string>();

        _logger.LogInformation("Delta mode: {Count} previously migrated files loaded", _deltaTracking.Count);
    }

    public bool ShouldSkip(FileRecord record)
    {
        if (!_settings.DeltaMode) return false;
        return _deltaTracking.TryGetValue(record.BlobPath, out var lastMod)
            && lastMod == record.LastModified;
    }

    public void TrackMigrated(FileRecord record)
    {
        _deltaTracking[record.BlobPath] = record.LastModified;
    }

    public void SaveDeltaTracking()
    {
        var json = JsonConvert.SerializeObject(_deltaTracking, Formatting.Indented);
        File.WriteAllText(_settings.DeltaTrackingFile, json);
    }

    public void WriteReport(List<MigrationResult> results)
    {
        using var writer = new StreamWriter(_settings.ReportFile);
        using var csv    = new CsvWriter(writer, CultureInfo.InvariantCulture);
        csv.WriteRecords(results);
        _logger.LogInformation("Report saved: {File}", _settings.ReportFile);
    }

    public void WriteFailedItems(List<MigrationResult> results)
    {
        var failed = results
            .Where(r => r.Status == "Failed")
            .Where(r => _settings.RetryIncludeAlreadyExists ||
                        !r.Error.Contains("already exists", StringComparison.OrdinalIgnoreCase))
            .Select(r => new FailedItemRow
            {
                SourceFile = r.SourceFile,
                DestPath = r.DestPath,
                Error = r.Error
            })
            .ToList();

        using var writer = new StreamWriter(_settings.FailedItemsFile);
        using var csv = new CsvWriter(writer, CultureInfo.InvariantCulture);
        csv.WriteRecords(failed);
        _logger.LogInformation("Failed-items file saved: {File} ({Count} rows)",
            _settings.FailedItemsFile, failed.Count);
    }

    public HashSet<string> LoadFailedSourceFiles()
    {
        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (!File.Exists(_settings.FailedItemsFile))
            return set;

        using var reader = new StreamReader(_settings.FailedItemsFile);
        using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
        var rows = csv.GetRecords<FailedItemRow>();
        foreach (var row in rows)
        {
            if (!string.IsNullOrWhiteSpace(row.SourceFile))
                set.Add(row.SourceFile);
        }
        return set;
    }

    public void PrintSummary(
        List<MigrationResult> results,
        List<FileRecord> skipped,
        int alreadyExistsConflicts = 0,
        int blobsListed = 0,
        int filesPlannedToMigrate = 0,
        int estimatedCaseFolders = 0,
        int otherErrorConflicts = 0,
        bool reportExistingFilesAsOverwritten = false,
        string? summaryYearLabel = null)
    {
        var success  = results.Count(r => r.Status == "Success");
        var partial  = results.Count(r => r.Status == "PartialSuccess");
        var failed   = results.Count(r => r.Status == "Failed");

        // Build summary block styled after Application 1.
        // Logged via _logger (appears in log files) AND Console.WriteLine (appears in terminal).
        var lines = new System.Text.StringBuilder();
        lines.AppendLine();
        lines.AppendLine("========== BlobToSharePointSync run summary ==========");
        if (!string.IsNullOrWhiteSpace(summaryYearLabel))
            lines.AppendLine($"  Year (YYYY scope):                   {summaryYearLabel}");
        if (blobsListed > 0)            lines.AppendLine($"  Blobs listed (container/prefix):     {blobsListed}");
        lines.AppendLine($"  Skipped (invalid/filtered):          {skipped.Count}");
        if (reportExistingFilesAsOverwritten && alreadyExistsConflicts > 0)
            lines.AppendLine($"  Already exists (reported as overwritten): {alreadyExistsConflicts}");
        else
            lines.AppendLine($"  Skipped (already exists in target):       {alreadyExistsConflicts}");
        if (filesPlannedToMigrate > 0) lines.AppendLine($"  Files planned to migrate:            {filesPlannedToMigrate}");
        lines.AppendLine($"  Files uploaded successfully:         {success + partial}  (this run only — per-blob row status)");
        lines.AppendLine($"  Failed uploads:                      {failed}");
        lines.AppendLine("  Note: SharePoint library item counts can exceed the rows above (previous migrations,");
        lines.AppendLine("         folders, or how the library UI counts items). SPMI queue \"FilesCreated\" is not a library census.");
        if (estimatedCaseFolders > 0)
            lines.AppendLine($"  Unique case folders in plan (YYYY/Case): {estimatedCaseFolders}");
        lines.AppendLine($"  Other errors (non-existence):        {otherErrorConflicts}");
        lines.AppendLine("======================================================");
        lines.AppendLine($"  Report saved:      {_settings.ReportFile}");
        lines.AppendLine($"  Failed-items file: {_settings.FailedItemsFile}");
        lines.Append    ("======================================================");

        var summary = lines.ToString();
        // Single logger emission avoids duplicate blocks when console + file sinks both capture output.
        _logger.LogInformation("{Summary}", summary);
        // Per-file detail is already captured in the CSV report files; omit it here to keep
        // console output clean and prevent 10k+ lines from burying the summary.
    }

    private sealed class FailedItemRow
    {
        public string SourceFile { get; set; } = string.Empty;
        public string DestPath { get; set; } = string.Empty;
        public string Error { get; set; } = string.Empty;
    }
}
