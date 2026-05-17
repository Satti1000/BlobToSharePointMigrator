using BlobToSharePointMigrator.Configuration;
using BlobToSharePointMigrator.Models;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Globalization;
using CsvHelper;

namespace BlobToSharePointMigrator.Services;

public sealed class RunArtifactsWritten
{
    public string? ReportPath { get; set; }
    public string? FailedItemsPath { get; set; }
    public string? OverwriteAuditPath { get; set; }
    public string? DeltaTrackingPath { get; set; }
}

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
        if (!_settings.DeltaMode)
            return;
        var json = JsonConvert.SerializeObject(_deltaTracking, Formatting.Indented);
        File.WriteAllText(_settings.DeltaTrackingFile, json);
    }

    public void WriteReport(List<MigrationResult> results)
    {
        using var writer = new StreamWriter(_settings.ReportFile);
        using var csv    = new CsvWriter(writer, CultureInfo.InvariantCulture);
        csv.WriteRecords(results);
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
    }

    public void WriteOverwriteAuditReport(IReadOnlyList<OverwriteAuditRow> rows)
    {
        using var writer = new StreamWriter(_settings.OverwriteAuditReportFile);
        using var csv = new CsvWriter(writer, CultureInfo.InvariantCulture);
        csv.WriteRecords(rows);
    }

    /// <summary>Writes CSV/JSON artifacts when needed; always leaves the run log as the primary human output.</summary>
    public RunArtifactsWritten WriteRunArtifacts(
        List<MigrationResult> results,
        IReadOnlyList<OverwriteAuditRow> auditRows)
    {
        var written = new RunArtifactsWritten();

        if (results.Count > 0)
        {
            WriteReport(results);
            written.ReportPath = _settings.ReportFile;
        }

        if (results.Any(r => r.Status == "Failed"))
        {
            WriteFailedItems(results);
            written.FailedItemsPath = _settings.FailedItemsFile;
        }
        else if (File.Exists(_settings.FailedItemsFile))
        {
            File.Delete(_settings.FailedItemsFile);
        }

        if (_settings.ReportExistingFilesAsOverwritten && auditRows.Count > 0)
        {
            WriteOverwriteAuditReport(auditRows);
            written.OverwriteAuditPath = _settings.OverwriteAuditReportFile;
        }

        if (_settings.DeltaMode)
        {
            SaveDeltaTracking();
            written.DeltaTrackingPath = _settings.DeltaTrackingFile;
        }

        return written;
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
        int spmiQueueAlreadyExistsEvents,
        int blobsListed,
        int filesPlannedToMigrate,
        int estimatedCaseFolders,
        int spmiQueueOtherErrors,
        bool reportExistingFilesAsOverwritten,
        string? summaryYearLabel,
        int wilberforceFileNameOnPlan,
        int wilberforceDateOnPlan,
        int overwriteAuditAlreadyExistsSignalRows = 0,
        int jobsSubmitted = 0,
        string? runLogPath = null,
        RunArtifactsWritten? artifacts = null,
        int? metadataPatchedCount = null)
    {
        var stats = RunSummaryStats.FromResults(
            results,
            skipped.Count,
            blobsListed,
            filesPlannedToMigrate,
            spmiQueueAlreadyExistsEvents,
            spmiQueueOtherErrors,
            overwriteAuditAlreadyExistsSignalRows,
            wilberforceFileNameOnPlan,
            wilberforceDateOnPlan);

        LogSummaryBlock(results, stats, summaryYearLabel, reportExistingFilesAsOverwritten, estimatedCaseFolders, jobsSubmitted,
            runLogPath, artifacts, metadataPatchedCount);

        if (!stats.CountsReconcile)
        {
            _logger.LogWarning(
                "Summary count mismatch: Success({Success}) + PartialSuccess({Partial}) + Failed({Failed}) + Other({Other}) != {ResultRows} result rows.",
                stats.Success, stats.PartialSuccess, stats.Failed, stats.OtherStatus, stats.ResultRows);
        }

        if (stats.PlannedWithoutResult > 0)
        {
            _logger.LogWarning(
                "Summary: {Missing} planned file(s) have no result row (not submitted or missing from batch results).",
                stats.PlannedWithoutResult);
        }
    }

    private void LogSummaryBlock(
        IReadOnlyList<MigrationResult> results,
        RunSummaryStats stats,
        string? summaryYearLabel,
        bool reportExistingFilesAsOverwritten,
        int estimatedCaseFolders,
        int jobsSubmitted,
        string? runLogPath,
        RunArtifactsWritten? artifacts,
        int? metadataPatchedCount)
    {
        void Line(string text) => _logger.LogInformation("{SummaryLine}", text);
        var reportPath = artifacts?.ReportPath ?? _settings.ReportFile;
        var failedListPath = artifacts?.FailedItemsPath ?? _settings.FailedItemsFile;

        Line(string.Empty);
        Line("========== BlobToSharePointSync — run summary ==========");
        if (!string.IsNullOrWhiteSpace(summaryYearLabel))
            Line($"  Year (YYYY scope):                   {summaryYearLabel}");
        if (jobsSubmitted > 0)
            Line($"  SPMI jobs submitted:                 {jobsSubmitted}");
        if (stats.BlobsListed > 0)
            Line($"  Blobs listed (inventory):            {stats.BlobsListed}");
        Line($"  Skipped (invalid/filtered):          {stats.SkippedInvalid}");
        if (stats.FilesPlanned > 0)
            Line($"  Files planned to migrate:            {stats.FilesPlanned}");
        if (estimatedCaseFolders > 0)
            Line($"  Estimated case folders (YYYY/nnn):   {estimatedCaseFolders}");
        Line($"  Per-file result rows:                {stats.ResultRows}");
        if (stats.PlannedWithoutResult > 0)
            Line($"  Planned with no result row:          {stats.PlannedWithoutResult}");
        Line(string.Empty);
        Line("  Per-file outcomes:");
        Line($"    Success:                           {stats.Success}");
        Line($"    PartialSuccess:                    {stats.PartialSuccess}");
        Line($"    Failed:                            {stats.Failed}");
        if (stats.OtherStatus > 0)
            Line($"    Other status (e.g. metadata-only): {stats.OtherStatus}");
        if (stats.Failed > 0)
        {
            if (stats.FailedSaveConflict > 0)
                Line($"      — Save Conflict:                 {stats.FailedSaveConflict}");
            if (stats.FailedDestinationAlreadyExists > 0)
                Line($"      — Destination already exists:    {stats.FailedDestinationAlreadyExists}");
            if (stats.FailedOther > 0)
                Line($"      — Other errors:                  {stats.FailedOther}");
        }
        Line($"  Uploaded (Success + PartialSuccess): {stats.Uploaded}");
        if (stats.CountsReconcile && stats.ResultRows > 0)
            Line($"  Count check:                       Success+Partial+Failed+Other = {stats.ResultRows} (matches {reportPath})");
        else if (stats.ResultRows > 0)
            Line("  Count check:                       WARNING — outcome counts do not add up to result rows; use report CSV.");
        if (stats.AllPlannedFailed)
            Line("  Note: Every planned file is Failed in this run.");
        LogPerFileOutcomeIndex(results, stats, reportPath, failedListPath, Line);
        Line(string.Empty);
        Line("  SPMI queue signals (batch-level, not per-file skips):");
        Line($"    Already-exists messages:           {stats.SpmiQueueAlreadyExistsEvents}");
        Line($"    Other non-existence errors:        {stats.SpmiQueueOtherErrors}");
        if (stats.OverwriteAuditAlreadyExistsSignalRows > 0)
            Line($"  Overwrite-audit batch already-exists signals: {stats.OverwriteAuditAlreadyExistsSignalRows}");
        if (reportExistingFilesAsOverwritten && stats.SpmiQueueAlreadyExistsEvents > 0)
            Line("  (ReportExistingFilesAsOverwritten=true: queue already-exists treated as overwrite intent.)");
        if (stats.WilberforceFileNameOnPlan > 0 || stats.WilberforceDateOnPlan > 0)
        {
            Line(string.Empty);
            Line("  Metadata on plan (before SharePoint patch):");
            Line($"    Wilberforce File Name:             {stats.WilberforceFileNameOnPlan}");
            Line($"    Wilberforce Date:                  {stats.WilberforceDateOnPlan}");
        }
        if (metadataPatchedCount.HasValue)
            Line($"  Metadata-only list items patched:  {metadataPatchedCount.Value}");
        Line(string.Empty);
        if (!string.IsNullOrWhiteSpace(runLogPath))
            Line($"  Run log:                           {runLogPath}");
        if (artifacts is not null)
        {
            var dataFiles = new List<string>();
            if (!string.IsNullOrWhiteSpace(artifacts.ReportPath)) dataFiles.Add(artifacts.ReportPath);
            if (!string.IsNullOrWhiteSpace(artifacts.FailedItemsPath)) dataFiles.Add(artifacts.FailedItemsPath);
            if (!string.IsNullOrWhiteSpace(artifacts.OverwriteAuditPath)) dataFiles.Add(artifacts.OverwriteAuditPath);
            if (!string.IsNullOrWhiteSpace(artifacts.DeltaTrackingPath)) dataFiles.Add(artifacts.DeltaTrackingPath);
            if (dataFiles.Count > 0)
                Line($"  Data exports (CSV/JSON):           {string.Join(", ", dataFiles)}");
        }
        Line("======================================================");
        Line("Migration complete.");
    }

    private static void LogPerFileOutcomeIndex(
        IReadOnlyList<MigrationResult> results,
        RunSummaryStats stats,
        string reportPath,
        string failedListPath,
        Action<string> line)
    {
        const int maxFailedLinesInLog = 40;

        line(string.Empty);
        line("  Per-file results (exact Status per row):");
        line($"    Report CSV:                        {reportPath}");
        line("      Status=Success         — file treated as migrated successfully");
        line("      Status=PartialSuccess  — SPMI completed with errors; row not Failed (metadata patch may still run)");
        line("      Status=Failed          — file failed for this run (see Error column)");

        if (stats.Uploaded > 0)
        {
            line($"    Succeeded (Success+Partial):         {stats.Uploaded} row(s) — open report CSV, filter Status column");
        }

        var failedRows = results
            .Where(r => string.Equals(r.Status, "Failed", StringComparison.OrdinalIgnoreCase))
            .ToList();
        if (failedRows.Count == 0)
            return;

        line($"    Failed:                              {failedRows.Count} row(s)");
        if (failedRows.Count <= maxFailedLinesInLog)
        {
            foreach (var r in failedRows)
            {
                var err = string.IsNullOrWhiteSpace(r.Error) ? "(no error text)" : TruncateError(r.Error, 160);
                line($"      FAILED | {r.SourceFile}");
                line($"             | {err}");
            }
        }
        else
        {
            foreach (var r in failedRows.Take(maxFailedLinesInLog))
            {
                var err = string.IsNullOrWhiteSpace(r.Error) ? "(no error text)" : TruncateError(r.Error, 120);
                line($"      FAILED | {r.SourceFile} | {err}");
            }
            line($"      ... and {failedRows.Count - maxFailedLinesInLog} more Failed row(s) in {failedListPath}");
        }
    }

    private static string TruncateError(string error, int maxLen) =>
        error.Length <= maxLen ? error : error[..maxLen] + "…";

    private sealed class RunSummaryStats
    {
        internal static RunSummaryStats FromResults(
            IReadOnlyList<MigrationResult> results,
            int skippedInvalid,
            int blobsListed,
            int filesPlanned,
            int spmiQueueAlreadyExistsEvents,
            int spmiQueueOtherErrors,
            int overwriteAuditAlreadyExistsSignalRows,
            int wilberforceFileNameOnPlan,
            int wilberforceDateOnPlan)
        {
            static bool ContainsAny(string? source, params string[] needles)
            {
                if (string.IsNullOrWhiteSpace(source))
                    return false;
                foreach (var needle in needles)
                {
                    if (source.Contains(needle, StringComparison.OrdinalIgnoreCase))
                        return true;
                }
                return false;
            }

            var success = 0;
            var partial = 0;
            var failed = 0;
            var otherStatus = 0;
            var failedSaveConflict = 0;
            var failedAlreadyExists = 0;
            var failedOther = 0;

            foreach (var r in results)
            {
                switch (r.Status)
                {
                    case "Success":
                        success++;
                        break;
                    case "PartialSuccess":
                        partial++;
                        break;
                    case "Failed":
                        failed++;
                        if (ContainsAny(r.Error, "save conflict", "conflict with those made concurrently"))
                            failedSaveConflict++;
                        else if (ContainsAny(r.Error, "already exists", "same name already exists", "a file with the same name"))
                            failedAlreadyExists++;
                        else
                            failedOther++;
                        break;
                    default:
                        otherStatus++;
                        break;
                }
            }

            return new RunSummaryStats
            {
                BlobsListed = blobsListed,
                SkippedInvalid = skippedInvalid,
                FilesPlanned = filesPlanned,
                ResultRows = results.Count,
                Success = success,
                PartialSuccess = partial,
                Failed = failed,
                FailedSaveConflict = failedSaveConflict,
                FailedDestinationAlreadyExists = failedAlreadyExists,
                FailedOther = failedOther,
                OtherStatus = otherStatus,
                SpmiQueueAlreadyExistsEvents = spmiQueueAlreadyExistsEvents,
                SpmiQueueOtherErrors = spmiQueueOtherErrors,
                OverwriteAuditAlreadyExistsSignalRows = overwriteAuditAlreadyExistsSignalRows,
                WilberforceFileNameOnPlan = wilberforceFileNameOnPlan,
                WilberforceDateOnPlan = wilberforceDateOnPlan
            };
        }

        public int BlobsListed { get; init; }
        public int SkippedInvalid { get; init; }
        public int FilesPlanned { get; init; }
        public int ResultRows { get; init; }
        public int Success { get; init; }
        public int PartialSuccess { get; init; }
        public int Failed { get; init; }
        public int FailedSaveConflict { get; init; }
        public int FailedDestinationAlreadyExists { get; init; }
        public int FailedOther { get; init; }
        public int OtherStatus { get; init; }
        public int SpmiQueueAlreadyExistsEvents { get; init; }
        public int SpmiQueueOtherErrors { get; init; }
        public int OverwriteAuditAlreadyExistsSignalRows { get; init; }
        public int WilberforceFileNameOnPlan { get; init; }
        public int WilberforceDateOnPlan { get; init; }
        public int Uploaded => Success + PartialSuccess;
        public int PlannedWithoutResult => Math.Max(0, FilesPlanned - ResultRows);
        public bool AllPlannedFailed => FilesPlanned > 0 && Failed == FilesPlanned && Uploaded == 0;
        public bool CountsReconcile =>
            ResultRows == 0 || Success + PartialSuccess + Failed + OtherStatus == ResultRows;
    }

    private sealed class FailedItemRow
    {
        public string SourceFile { get; set; } = string.Empty;
        public string DestPath { get; set; } = string.Empty;
        public string Error { get; set; } = string.Empty;
    }
}
