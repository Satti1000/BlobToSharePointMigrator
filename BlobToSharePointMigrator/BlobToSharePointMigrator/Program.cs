using BlobToSharePointMigrator.Configuration;
using BlobToSharePointMigrator.Models;
using BlobToSharePointMigrator.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Serilog;
using System.Collections.Concurrent;
using System.Text.RegularExpressions;

Console.WriteLine();
Console.WriteLine("===================================================");
Console.WriteLine("   Blob-to-SharePoint ETL Migration Pipeline");
Console.WriteLine("   .NET 8 | SharePoint Migration API");
Console.WriteLine("===================================================");
Console.WriteLine();

var config = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: false)
    .AddEnvironmentVariables()
    .Build();

var configurationForSerilog = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
    .AddEnvironmentVariables()
    .Build();

Log.Logger = new LoggerConfiguration()
    .ReadFrom.Configuration(configurationForSerilog)
    .CreateLogger();

var migrationSection = config.GetSection("Migration");
if (!migrationSection.Exists())
    throw new InvalidOperationException("'Migration' section was not found in appsettings.json");

var migrationSettings = migrationSection.Get<MigrationSettings>()
    ?? throw new InvalidOperationException("Migration settings could not be bound from the 'Migration' section.");

var services = new ServiceCollection()
    .AddLogging(b => b
        .ClearProviders()
        .AddSerilog(Log.Logger, dispose: false)
        .SetMinimumLevel(LogLevel.Information))
    .BuildServiceProvider();

var loggerFactory = services.GetRequiredService<ILoggerFactory>();

var blobService = new BlobInventoryService(migrationSettings, loggerFactory.CreateLogger<BlobInventoryService>());
var transformSvc = new PathTransformService(
    Path.Combine(AppContext.BaseDirectory, migrationSettings.MappingFile),
    migrationSettings.UseYyyyCaseNumberPath,
    loggerFactory.CreateLogger<PathTransformService>(),
    migrationSettings.BlobFolderPrefix,
    migrationSettings.SharePointTargetFolder);
var spServiceProbe = new SharePointMigrationService(migrationSettings, loggerFactory.CreateLogger<SharePointMigrationService>());
var reportSvc = new ReportService(migrationSettings, loggerFactory.CreateLogger<ReportService>());
var caseMetadataSvc = new CaseDocumentMetadataService(migrationSettings, loggerFactory.CreateLogger<CaseDocumentMetadataService>());

var logger = loggerFactory.CreateLogger("Pipeline");

// Client visibility: always emit BlobFolderPrefix at start of the log (empty = no prefix filter on inventory).
var blobFolderPrefixLogValue = string.IsNullOrWhiteSpace(migrationSettings.BlobFolderPrefix)
    ? "(empty — no BlobFolderPrefix filter; full container inventory per other settings)"
    : migrationSettings.BlobFolderPrefix.Replace('\\', '/').TrimEnd('/');
logger.LogInformation("Migration:BlobFolderPrefix = {BlobFolderPrefix}", blobFolderPrefixLogValue);

static void ValidateStartupSettings(MigrationSettings migrationSettings)
{
    var errors = new List<string>();

    if (string.IsNullOrWhiteSpace(migrationSettings.BlobConnectionString))
        errors.Add("Migration:BlobConnectionString is empty.");
    if (string.IsNullOrWhiteSpace(migrationSettings.SourceContainer))
        errors.Add("Migration:SourceContainer is empty.");
    if (string.IsNullOrWhiteSpace(migrationSettings.SharePointSiteUrl))
        errors.Add("Migration:SharePointSiteUrl is empty.");
    if (!migrationSettings.YearAsLibrary && string.IsNullOrWhiteSpace(migrationSettings.SharePointDocumentLibrary))
        errors.Add("Migration:SharePointDocumentLibrary is empty (use exact library title like 'Documents' or 'Shared Documents').");
    if (string.IsNullOrWhiteSpace(migrationSettings.SharePointClientId))
        errors.Add("Migration:SharePointClientId is empty.");
    if (string.IsNullOrWhiteSpace(migrationSettings.SharePointTenantId))
        errors.Add("Migration:SharePointTenantId is empty.");
    if (string.IsNullOrWhiteSpace(migrationSettings.SharePointCertificatePath) &&
        string.IsNullOrWhiteSpace(migrationSettings.SharePointCertificateThumbprint))
        errors.Add("Migration certificate is not configured. Set Migration:SharePointCertificatePath (+Password) or Migration:SharePointCertificateThumbprint.");
    if ((migrationSettings.MigrationYearStart > 0 || migrationSettings.MigrationYearEnd > 0) &&
        migrationSettings.MigrationYear <= 0 &&
        (migrationSettings.MigrationYearStart <= 0 || migrationSettings.MigrationYearEnd <= 0))
        errors.Add("Set both Migration:MigrationYearStart and Migration:MigrationYearEnd for a year range, or leave both as 0.");
    if (migrationSettings.MigrationYearStart > 0 &&
        migrationSettings.MigrationYearEnd > 0 &&
        migrationSettings.MigrationYearStart > migrationSettings.MigrationYearEnd)
        errors.Add("Migration:MigrationYearStart cannot be greater than Migration:MigrationYearEnd.");
    if (migrationSettings.MigrationYear > 0 &&
        (migrationSettings.MigrationYearStart > 0 || migrationSettings.MigrationYearEnd > 0))
        errors.Add("Use either Migration:MigrationYear for a single year or Migration:MigrationYearStart/End for a range, not both.");

    if (errors.Count > 0)
        throw new InvalidOperationException("Startup settings validation failed:\n - " + string.Join("\n - ", errors));
}

// Strips "YYYY/" library prefix from a mapped path so DestPath/SharePointUrl
// in the report reflects the path-within-library, not the full YYYY/case/file path.
static string StripLibraryPrefix(string mappedPath, string libraryPrefix)
{
    if (string.IsNullOrWhiteSpace(libraryPrefix))
        return mappedPath;
    var normalized = (mappedPath ?? string.Empty).Replace('\\', '/');
    var prefix = libraryPrefix.Trim('/') + "/";
    return normalized.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)
        ? normalized[prefix.Length..]
        : normalized;
}

static string BuildSummaryYearLabel(MigrationSettings s, List<FileRecord> files)
{
    if (TryGetConfiguredYearScope(s, out var configuredStart, out var configuredEnd))
        return configuredStart == configuredEnd
            ? configuredStart.ToString()
            : $"{configuredStart}-{configuredEnd}";
    if (files.Count == 0)
        return "—";
    var years = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    foreach (var r in files)
    {
        var path = (r.MappedPath ?? string.Empty).Replace('\\', '/').Trim('/');
        var segs = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (segs.Length > 0 && Regex.IsMatch(segs[0], "^\\d{4}$"))
            years.Add(segs[0]);
    }
    if (years.Count == 0)
        return "—";
    var ordered = years.OrderBy(y => y).ToList();
    if (ordered.Count == 1)
        return ordered[0];
    if (ordered.Count <= 5)
        return string.Join(", ", ordered);
    return string.Join(", ", ordered.Take(5)) + ", …";
}

static bool TryGetConfiguredYearScope(MigrationSettings s, out int startYear, out int endYear)
{
    if (s.MigrationYear > 0)
    {
        startYear = s.MigrationYear;
        endYear = s.MigrationYear;
        return true;
    }

    if (s.MigrationYearStart > 0 && s.MigrationYearEnd > 0)
    {
        startYear = s.MigrationYearStart;
        endYear = s.MigrationYearEnd;
        return true;
    }

    startYear = 0;
    endYear = 0;
    return false;
}

static bool TryGetMappedPathYear(FileRecord record, out int year)
{
    var path = (record.MappedPath ?? string.Empty).Replace('\\', '/').Trim('/');
    var segs = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
    if (segs.Length > 0 && int.TryParse(segs[0], out year) && year is >= 1000 and <= 9999)
        return true;

    year = 0;
    return false;
}

static bool MigrationJobHasSaveConflictErrors(MigrationJobInfo info)
{
    foreach (var e in info.Errors)
    {
        if (e.Contains("Save Conflict", StringComparison.OrdinalIgnoreCase))
            return true;
        if (e.Contains("conflict with those made concurrently", StringComparison.OrdinalIgnoreCase))
            return true;
    }
    return false;
}

static string ExtractCaseFolderFromMappedPath(string? mappedPath)
{
    var path = (mappedPath ?? string.Empty).Replace('\\', '/').Trim('/');
    var segs = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
    if (segs.Length >= 2 && Regex.IsMatch(segs[0], "^\\d{4}$") && Regex.IsMatch(segs[1], "^\\d+$"))
        return $"{segs[0]}/{segs[1]}";
    if (segs.Length >= 2)
        return $"{segs[0]}/{segs[1]}";
    if (segs.Length == 1)
        return segs[0];
    return string.Empty;
}

async Task<int> PatchCaseMetadataForExistingFilesAsync(IReadOnlyList<FileRecord> recordsToPatch, string stepLabel)
{
    if (recordsToPatch.Count == 0)
        return 0;

    var metaService = new SharePointMigrationService(migrationSettings, loggerFactory.CreateLogger<SharePointMigrationService>());
    await metaService.ConnectAsync();

    var totalPatched = 0;
    if (migrationSettings.YearAsLibrary)
    {
        foreach (var g in recordsToPatch.GroupBy(r =>
        {
            var segs = (r.MappedPath ?? string.Empty).Replace('\\', '/').Trim('/')
                .Split('/', StringSplitOptions.RemoveEmptyEntries);
            return segs.Length > 0 && Regex.IsMatch(segs[0], @"^\d{4}$") ? segs[0] : string.Empty;
        }, StringComparer.OrdinalIgnoreCase))
        {
            if (string.IsNullOrEmpty(g.Key))
            {
                logger.LogWarning("{Step} - Skipping {Count} metadata-only record(s) because no YYYY library prefix was found in the mapped path.",
                    stepLabel, g.Count());
                continue;
            }

            var patched = await metaService.PatchCaseMetadataBulkAsync(g.ToList(), g.Key, g.Key);
            totalPatched += patched;
            logger.LogInformation("{Step} - Library {Library}: patched {Count} existing list items.",
                stepLabel, g.Key, patched);
        }
    }
    else if (!string.IsNullOrWhiteSpace(migrationSettings.SharePointDocumentLibrary))
    {
        totalPatched = await metaService.PatchCaseMetadataBulkAsync(
            recordsToPatch.ToList(), migrationSettings.SharePointDocumentLibrary, yearPrefixToStrip: null);
        logger.LogInformation("{Step} - Library {Library}: patched {Count} existing list items.",
            stepLabel, migrationSettings.SharePointDocumentLibrary, totalPatched);
    }
    else
    {
        logger.LogWarning("{Step} - No SharePoint library configured; metadata patch skipped.", stepLabel);
    }

    return totalPatched;
}

try
{
    logger.LogInformation("Running startup configuration precheck...");
    ValidateStartupSettings(migrationSettings);
    logger.LogInformation("Startup precheck passed.");

    reportSvc.LoadDeltaTracking();

    logger.LogInformation("STEP 1/5 - Inventorying Azure Blob Storage...");
    var records = await blobService.InventoryAsync();

    // Optional source prefilter: BlobFolderPrefix
    if (!string.IsNullOrWhiteSpace(migrationSettings.BlobFolderPrefix))
    {
        var normalizedPrefix = migrationSettings.BlobFolderPrefix
            .Replace('\\', '/')
            .Trim('/');
        var prefixWithSlash = normalizedPrefix + "/";
        var before = records.Count;
        records = records.Where(r =>
        {
            var path = (r.BlobPath ?? string.Empty).Replace('\\', '/').Trim('/');
            return path.Equals(normalizedPrefix, StringComparison.OrdinalIgnoreCase) ||
                   path.StartsWith(prefixWithSlash, StringComparison.OrdinalIgnoreCase);
        }).ToList();
        logger.LogInformation("Applied BlobFolderPrefix filter: kept {Kept} of {Before} records under '{Prefix}'",
            records.Count, before, migrationSettings.BlobFolderPrefix);
    }

    logger.LogInformation("STEP 2/5 - Transforming folder paths...");
    transformSvc.TransformAll(records);

    var allowed = records.Where(r => r.IsAllowed).ToList();
    var skipped = records.Where(r => !r.IsAllowed).ToList();

    foreach (var s in skipped)
        logger.LogInformation("Skipped: {File} ({Reason})", s.BlobPath, s.SkipReason);

    var toMigrate = migrationSettings.MetadataOnlyMode
        ? allowed.ToList()
        : allowed.Where(r => !reportSvc.ShouldSkip(r)).ToList();
    if (migrationSettings.MetadataOnlyMode && migrationSettings.DeltaMode)
    {
        logger.LogInformation("MetadataOnlyMode is enabled; DeltaMode skip is ignored so existing SharePoint files can be patched.");
    }

    // Optional year filter on mapped destination (YYYY/CaseNumber/...)
    if (TryGetConfiguredYearScope(migrationSettings, out var filterStartYear, out var filterEndYear) &&
        migrationSettings.UseYyyyCaseNumberPath)
    {
        var before = toMigrate.Count;
        toMigrate = toMigrate
            .Where(r => TryGetMappedPathYear(r, out var year) && year >= filterStartYear && year <= filterEndYear)
            .ToList();

        var scopeLabel = filterStartYear == filterEndYear
            ? filterStartYear.ToString()
            : $"{filterStartYear}-{filterEndYear}";
        logger.LogInformation("Applied migration year scope filter: kept {Kept} of {Before} files for year scope {YearScope}",
            toMigrate.Count, before, scopeLabel);
    }
    if (migrationSettings.MaxFilesToMigrate > 0 && toMigrate.Count > migrationSettings.MaxFilesToMigrate)
    {
        toMigrate = toMigrate.Take(migrationSettings.MaxFilesToMigrate).ToList();
        logger.LogInformation("Milestone cap active: limiting run to first {Limit} files.", migrationSettings.MaxFilesToMigrate);
    }

    if (migrationSettings.RetryFailedOnly)
    {
        var failedSet = reportSvc.LoadFailedSourceFiles();
        var before = toMigrate.Count;
        toMigrate = toMigrate.Where(r => failedSet.Contains(r.BlobPath)).ToList();
        logger.LogInformation("RetryFailedOnly enabled: kept {Kept} of {Before} files from {FailedItemsFile}.",
            toMigrate.Count, before, migrationSettings.FailedItemsFile);
    }

    logger.LogInformation("Files to migrate (after delta): {Count} of {Total}", toMigrate.Count, allowed.Count);

    logger.LogInformation("STEP 2.5/5 - Enriching CaseId, CaseType, and DocumentId (paths; DocumentId from case_NNN_documents.xml when AssignDocumentIdFromCaseXml is true)...");
    await caseMetadataSvc.EnrichAsync(toMigrate, records, blobService.DownloadBlobAsync);

    // Estimate unique case-folder count (YYYY/CaseNumber) when that mapping mode is active.
    if (migrationSettings.UseYyyyCaseNumberPath)
    {
        var caseFolders = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var r in toMigrate)
        {
            var path = (r.MappedPath ?? string.Empty).Replace('\\', '/').Trim('/');
            if (string.IsNullOrWhiteSpace(path)) continue;
            var segs = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
            if (segs.Length >= 2 &&
                Regex.IsMatch(segs[0], "^\\d{4}$") &&
                Regex.IsMatch(segs[1], "^\\d+$"))
            {
                caseFolders.Add($"{segs[0]}/{segs[1]}");
            }
        }
        logger.LogInformation("Estimated unique case folders to create/update: {Count} (from {Files} files)",
            caseFolders.Count, toMigrate.Count);
    }

    if (toMigrate.Count == 0)
    {
        logger.LogInformation(migrationSettings.MetadataOnlyMode
            ? "No files to patch (all records were filtered or invalid)."
            : "No files to migrate (all already migrated or filtered)");
        reportSvc.SaveDeltaTracking();
        reportSvc.PrintSummary(new List<MigrationResult>(), skipped, 0, records.Count, toMigrate.Count, 0, 0,
            migrationSettings.ReportExistingFilesAsOverwritten, BuildSummaryYearLabel(migrationSettings, toMigrate));
        Environment.Exit(0);
    }

    if (migrationSettings.MetadataOnlyMode)
    {
        logger.LogInformation("MetadataOnlyMode enabled: skipping SPMI upload/copy and patching metadata on existing SharePoint files by mapped path.");
        var patched = await PatchCaseMetadataForExistingFilesAsync(toMigrate, "METADATA-ONLY");

        var metadataOnlyResults = toMigrate.Select(record =>
        {
            var targetLibrary = migrationSettings.YearAsLibrary
                ? ((record.MappedPath ?? string.Empty).Replace('\\', '/').Trim('/')
                    .Split('/', StringSplitOptions.RemoveEmptyEntries).FirstOrDefault() ?? string.Empty)
                : migrationSettings.SharePointDocumentLibrary ?? string.Empty;
            var destPath = StripLibraryPrefix(record.MappedPath ?? string.Empty, targetLibrary);
            var documentIdPresent = record.Metadata.TryGetValue("DocumentId", out var did) && !string.IsNullOrWhiteSpace(did);

            return new MigrationResult
            {
                SourceFile = record.BlobPath,
                DestPath = destPath,
                SizeBytes = record.SizeBytes,
                LastModified = record.LastModified,
                Status = "MetadataPatchAttempted",
                SharePointUrl = $"{migrationSettings.SharePointSiteUrl.TrimEnd('/')}/{targetLibrary}/{destPath}",
                Error = documentIdPresent ? string.Empty : "DocumentId missing after manifest matching; CaseId/CaseType patch may still have been attempted.",
                Duration = "N/A (metadata-only)"
            };
        }).ToList();

        reportSvc.WriteReport(metadataOnlyResults);
        logger.LogInformation("Metadata-only complete: attempted {Attempted} files, patched {Patched} existing SharePoint list items. No files were uploaded or overwritten.",
            toMigrate.Count, patched);
        reportSvc.PrintSummary(metadataOnlyResults, skipped, 0, records.Count, toMigrate.Count, 0, 0,
            migrationSettings.ReportExistingFilesAsOverwritten, BuildSummaryYearLabel(migrationSettings, toMigrate));
        Environment.Exit(0);
    }

    logger.LogInformation("STEP 3/5 - Connecting to SharePoint...");
    await spServiceProbe.ConnectAsync();

    // STEP 4/5 and 5/5 — optionally partition into multiple jobs to improve SharePoint-side throughput.
    // Simple heuristic: when using YYYY/CaseNumber mapping and file count is large, batch by case folder,
    // with a soft cap per job.
    const int MaxFilesPerJob = 2000;
    var enablePartitioning = migrationSettings.UseYyyyCaseNumberPath && toMigrate.Count > MaxFilesPerJob;

    List<List<BlobToSharePointMigrator.Models.FileRecord>> BuildBatches(List<BlobToSharePointMigrator.Models.FileRecord> files)
    {
        if (!enablePartitioning)
            return new List<List<BlobToSharePointMigrator.Models.FileRecord>> { files };

        var byCase = new Dictionary<string, List<BlobToSharePointMigrator.Models.FileRecord>>(StringComparer.OrdinalIgnoreCase);
        foreach (var r in files)
        {
            var path = (r.MappedPath ?? string.Empty).Replace('\\', '/').Trim('/');
            var segs = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
            string key = "misc";
            if (segs.Length >= 2 && Regex.IsMatch(segs[0], "^\\d{4}$") && Regex.IsMatch(segs[1], "^\\d+$"))
                key = $"{segs[0]}/{segs[1]}";

            if (!byCase.TryGetValue(key, out var list))
            {
                list = new List<BlobToSharePointMigrator.Models.FileRecord>();
                byCase[key] = list;
            }
            list.Add(r);
        }

        // Pack case groups into jobs of ~MaxFilesPerJob
        var batches = new List<List<BlobToSharePointMigrator.Models.FileRecord>>();
        var current = new List<BlobToSharePointMigrator.Models.FileRecord>();
        foreach (var kvp in byCase)
        {
            var group = kvp.Value;
            if (current.Count + group.Count > MaxFilesPerJob && current.Count > 0)
            {
                batches.Add(current);
                current = new List<BlobToSharePointMigrator.Models.FileRecord>();
            }
            current.AddRange(group);
        }
        if (current.Count > 0) batches.Add(current);
        return batches;
    }

    var batchesToRun = BuildBatches(toMigrate);
    logger.LogInformation("Submitting {BatchCount} migration job(s) ({Total} files total)...", batchesToRun.Count, toMigrate.Count);
    logger.LogInformation(
        "Migration:EnableMigrationJobSaveConflictRetry = {Enabled}. When true, Save Conflict resubmit uses MigrationJobSaveConflictRetries={Retries}, delay {Delay}s.",
        migrationSettings.EnableMigrationJobSaveConflictRetry,
        migrationSettings.MigrationJobSaveConflictRetries,
        migrationSettings.MigrationJobSaveConflictRetryDelaySeconds);
    logger.LogInformation(
        "Migration:TreatIncompleteSpmiBatchAsPartialSuccessForMetadata = {Enabled}. When true, CompletedWithErrors + FilesCreated below batch size (without Save Conflict) still yields PartialSuccess so STEP 5 metadata patch runs.",
        migrationSettings.TreatIncompleteSpmiBatchAsPartialSuccessForMetadata);

    var allResults = new List<BlobToSharePointMigrator.Models.MigrationResult>();
    var aggregateAlreadyExists = 0;
    var aggregateOtherErrors = 0;
    var batchAuditBySourceFile = new ConcurrentDictionary<string, (bool AlreadyExistsSignal, string JobStatus, int FilesCreated, int SubmittedCount, string Note)>(StringComparer.OrdinalIgnoreCase);
    var deltaLock = new object();

    // Run with limited parallelism across batches; also serialize SPMI jobs that target the same
    // document library (e.g. same YYYY) to reduce SharePoint Save Conflict (JobError) from concurrent imports.
    var parallelJobs = Math.Max(1, migrationSettings.MaxParallelJobs);
    var libraryJobSemaphores = new ConcurrentDictionary<string, SemaphoreSlim>(StringComparer.OrdinalIgnoreCase);
    using (var gate = new SemaphoreSlim(parallelJobs))
    {
        var tasks = new List<Task>();
        for (int i = 0; i < batchesToRun.Count; i++)
        {
            var index = i;
            var batch = batchesToRun[i];
            await gate.WaitAsync();
            tasks.Add(Task.Run(async () =>
            {
                string targetLibrary = migrationSettings.YearAsLibrary
                    ? ((batch.Select(b => (b.MappedPath ?? string.Empty).Replace('\\', '/').Trim('/').Split('/', StringSplitOptions.RemoveEmptyEntries).FirstOrDefault())
                        .FirstOrDefault())?.Trim() ?? string.Empty)
                    : migrationSettings.SharePointDocumentLibrary ?? string.Empty;
                var libraryLockKey = string.IsNullOrWhiteSpace(targetLibrary) ? "__defaultLibrary" : targetLibrary;
                var libSem = libraryJobSemaphores.GetOrAdd(libraryLockKey, _ => new SemaphoreSlim(1, 1));
                await libSem.WaitAsync();
                try
                {
                    // Try to extract a single case folder id (YYYY/CaseNumber) for nicer progress logs
                    string caseLabel = "";
                    var cases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    foreach (var r in batch)
                    {
                        var p = (r.MappedPath ?? string.Empty).Replace('\\', '/').Trim('/');
                        var seg = p.Split('/', StringSplitOptions.RemoveEmptyEntries);
                        if (seg.Length >= 2 && Regex.IsMatch(seg[0], "^\\d{4}$") && Regex.IsMatch(seg[1], "^\\d+$"))
                            cases.Add($"{seg[0]}/{seg[1]}");
                        if (cases.Count > 2) break;
                    }
                    caseLabel = cases.Count == 1 ? cases.First() : (cases.Count > 1 ? $"{cases.Count} cases" : "mixed");
                    var sampleFile = batch.FirstOrDefault()?.BlobPath ?? string.Empty;
                    var caseIdAssignedCount = batch.Count(r => r.Metadata.TryGetValue("CaseId", out var value) && !string.IsNullOrWhiteSpace(value));
                    var caseTypeAssignedCount = batch.Count(r => r.Metadata.TryGetValue("CaseType", out var value) && !string.IsNullOrWhiteSpace(value));
                    var documentIdAssignedCount = batch.Count(r => r.Metadata.TryGetValue("DocumentId", out var value) && !string.IsNullOrWhiteSpace(value));

                    logger.LogInformation(
                        "Submitting job {Index}/{TotalJobs} for {Count} files — state: processing {Case} — metadata: CaseId={CaseIdCount}, CaseType={CaseTypeCount}, DocumentId={DocumentIdCount} — sample file: {File} — library: {Library}",
                        index + 1,
                        batchesToRun.Count,
                        batch.Count,
                        caseLabel,
                        caseIdAssignedCount,
                        caseTypeAssignedCount,
                        documentIdAssignedCount,
                        sampleFile,
                        targetLibrary);
                    // Use isolated service/context per concurrent batch to avoid shared-state contention.
                    var batchSpService = new SharePointMigrationService(migrationSettings, loggerFactory.CreateLogger<SharePointMigrationService>());
                    await batchSpService.ConnectAsync();
                    // If using Year-as-Library, strip leading "YYYY/" from mapped paths for this batch
                    var batchForSubmit = batch;
                    if (migrationSettings.YearAsLibrary && !string.IsNullOrWhiteSpace(targetLibrary))
                    {
                        var prefixToTrim = targetLibrary + "/";
                        batchForSubmit = batch.Select(r =>
                        {
                            var cloned = new FileRecord
                            {
                                Name = r.Name,
                                BlobPath = r.BlobPath,
                                MappedPath = r.MappedPath.Replace('\\', '/').StartsWith(prefixToTrim, StringComparison.OrdinalIgnoreCase)
                                    ? r.MappedPath.Replace('\\', '/')[prefixToTrim.Length..]
                                    : r.MappedPath.Replace('\\', '/'),
                                SizeBytes = r.SizeBytes,
                                ContentType = r.ContentType,
                                LastModified = r.LastModified,
                                CreatedOn = r.CreatedOn,
                                IsAllowed = r.IsAllowed,
                                SkipReason = r.SkipReason,
                                Metadata = r.Metadata,
                                FolderMetadata = r.FolderMetadata
                            };
                            return cloned;
                        }).ToList();
                    }

                    var pollIntervalSeconds = Math.Max(1, migrationSettings.JobPollIntervalSeconds);
                    var timeoutMinutes = Math.Max(1, migrationSettings.JobTimeoutMinutes);
                    var maxSaveConflictRetries = migrationSettings.EnableMigrationJobSaveConflictRetry
                        ? Math.Max(0, migrationSettings.MigrationJobSaveConflictRetries)
                        : 0;
                    var retryDelaySec = Math.Max(5, migrationSettings.MigrationJobSaveConflictRetryDelaySeconds);

                    MigrationJobInfo finalJobInfo = null!;
                    for (var attempt = 0; ; attempt++)
                    {
                        var jobInfo = await batchSpService.SubmitMigrationJobAsync(batchForSubmit, blobService.DownloadBlobAsync, targetLibrary);
                        logger.LogInformation("Migration job submitted: {JobId} (submit attempt {Attempt})", jobInfo.JobId, attempt + 1);

                        finalJobInfo = await batchSpService.PollMigrationJobAsync(
                            jobInfo.JobId,
                            TimeSpan.FromMinutes(timeoutMinutes),
                            pollIntervalSeconds * 1000,
                            batch.Count);
                        await batchSpService.CleanupStagingContainersAsync();

                        var saveConflict = MigrationJobHasSaveConflictErrors(finalJobInfo);
                        var underProcessed = finalJobInfo.ProcessedFileCount < batch.Count;
                        var badStatus = finalJobInfo.Status == "CompletedWithErrors" || finalJobInfo.Status == "Failed";
                        var canRetry = attempt < maxSaveConflictRetries && saveConflict && underProcessed && badStatus;

                        if (!canRetry)
                            break;

                        logger.LogWarning(
                            "SPMI Save Conflict (JobError); resubmitting after {Delay}s. Processed={Processed}/{Total}. Retries used: {Attempt}/{Max}. See: https://sharepoint.stackexchange.com/q/184207",
                            retryDelaySec, finalJobInfo.ProcessedFileCount, batch.Count, attempt + 1, maxSaveConflictRetries);
                        await Task.Delay(TimeSpan.FromSeconds(retryDelaySec));
                    }

                    Interlocked.Add(ref aggregateAlreadyExists, finalJobInfo.AlreadyExistsCount);
                    Interlocked.Add(ref aggregateOtherErrors, finalJobInfo.OtherErrorCount);

                    // Batch failure: trust SPMI queue summary. Destination-already-present cases are
                    // classified as non-fatal in SharePointMigrationService (JobFatalError + conflict text)
                    // so Status should not be Failed for those re-runs.
                    //
                    // SPMI "Processed" is queue FilesCreated, not a per-file audit. If the job completed with
                    // errors but processed fewer objects than we submitted, do not mark every blob as
                    // PartialSuccess — that overstates success vs SharePoint (library counts / failures per file).
                    var spmiIncomplete =
                        string.Equals(finalJobInfo.Status, "CompletedWithErrors", StringComparison.OrdinalIgnoreCase) &&
                        finalJobInfo.ProcessedFileCount < batch.Count;
                    var hardBatchFailed = finalJobInfo.Status == "Failed" ||
                        (string.Equals(finalJobInfo.Status, "CompletedWithErrors", StringComparison.OrdinalIgnoreCase) &&
                         finalJobInfo.ProcessedFileCount == 0);
                    var saveConflictBatch = MigrationJobHasSaveConflictErrors(finalJobInfo);
                    var treatIncompleteAsPartialForMetadata =
                        migrationSettings.TreatIncompleteSpmiBatchAsPartialSuccessForMetadata &&
                        spmiIncomplete &&
                        !hardBatchFailed &&
                        !saveConflictBatch;
                    if (treatIncompleteAsPartialForMetadata)
                    {
                        logger.LogWarning(
                            "Job {Index}/{TotalJobs}: SPMI FilesCreated={Processed} is below batch size {Batch}; TreatIncompleteSpmiBatchAsPartialSuccessForMetadata=true — marking rows PartialSuccess so metadata patch (DocumentId, etc.) still runs. Confirm SharePoint outcomes before relying on CSV status.",
                            index + 1,
                            batchesToRun.Count,
                            finalJobInfo.ProcessedFileCount,
                            batch.Count);
                    }

                    var markAllFailed = hardBatchFailed || (spmiIncomplete && !treatIncompleteAsPartialForMetadata);
                    var batchAlreadyExistsSignal = finalJobInfo.AlreadyExistsCount > 0;
                    var batchAuditNote = batchAlreadyExistsSignal
                        ? "SPMI queue reported destination already-exists signal(s); treat as overwrite/replacement intent."
                        : "No destination already-exists signal reported for this batch.";
                    var firstError = finalJobInfo.Errors
                        .FirstOrDefault(e =>
                            e.Contains("JobFatalError", StringComparison.OrdinalIgnoreCase) ||
                            e.Contains("JobError", StringComparison.OrdinalIgnoreCase) ||
                            e.Contains("Fatal", StringComparison.OrdinalIgnoreCase) ||
                            e.Contains("not found", StringComparison.OrdinalIgnoreCase))
                        ?? finalJobInfo.Errors.FirstOrDefault()
                        ?? string.Empty;

                    foreach (var record in batch)
                    {
                        batchAuditBySourceFile[record.BlobPath] = (
                            batchAlreadyExistsSignal,
                            finalJobInfo.Status,
                            finalJobInfo.ProcessedFileCount,
                            batch.Count,
                            batchAuditNote);

                        var rowStatus = markAllFailed
                            ? "Failed"
                            : finalJobInfo.Status == "Completed"
                                ? "Success"
                                : finalJobInfo.Status == "CompletedWithErrors"
                                    ? "PartialSuccess"
                                    : "Failed";

                        // Strip leading "YYYY/" from DestPath/SharePointUrl when YearAsLibrary is active
                        // so the report shows the correct path-within-library (no double year).
                        var destPath = StripLibraryPrefix(record.MappedPath, targetLibrary);

                        logger.LogInformation("Destination PAth : {destPath}", destPath);

                        var result = new BlobToSharePointMigrator.Models.MigrationResult
                        {
                            SourceFile = record.BlobPath,
                            DestPath = destPath,
                            SizeBytes = record.SizeBytes,
                            LastModified = record.LastModified,
                            Status = rowStatus,
                            SharePointUrl = $"{migrationSettings.SharePointSiteUrl.TrimEnd('/')}/{targetLibrary}/{destPath}",
                            Error = rowStatus == "Failed" ? firstError : string.Empty,
                            Duration = "N/A (batch operation)"
                        };

                        lock (allResults)
                        {
                            allResults.Add(result);
                        }

                        if (result.Status == "Success" || result.Status == "PartialSuccess")
                        {
                            lock (deltaLock)
                            {
                                reportSvc.TrackMigrated(record);
                            }
                        }
                    }

                    logger.LogInformation(
                        "Job {Index}/{TotalJobs} complete: Status={Status}, SPMI queue FilesCreated={Processed}/{Total} (SPMI counter — not a SharePoint library file inventory; library totals can differ from earlier runs or folders).",
                        index + 1, batchesToRun.Count, finalJobInfo.Status, finalJobInfo.ProcessedFileCount, finalJobInfo.TotalFileCount);
                    if (spmiIncomplete && !treatIncompleteAsPartialForMetadata)
                    {
                        logger.LogWarning(
                            "Job {Index}/{TotalJobs}: SPMI reported fewer created objects ({Processed}) than files in this batch ({Batch}). Per-file outcomes are not available from the queue; marking all rows Failed for this batch in the CSV/report. Set Migration:TreatIncompleteSpmiBatchAsPartialSuccessForMetadata=true (and ensure no Save Conflict) if reruns should still refresh DocumentId via STEP 5.",
                            index + 1, batchesToRun.Count, finalJobInfo.ProcessedFileCount, batch.Count);
                    }
                }
                catch (Exception ex)
                {
                    // Do not fail the full pipeline when a single batch fails; continue other batches.
                    logger.LogError(ex, "Job {Index}/{TotalJobs} failed. Continuing with remaining batches.",
                        index + 1, batchesToRun.Count);

                    foreach (var record in batch)
                    {
                        var catchLibrary = migrationSettings.YearAsLibrary
                            ? (record.MappedPath.Replace('\\', '/').Trim('/').Split('/', StringSplitOptions.RemoveEmptyEntries).FirstOrDefault() ?? string.Empty)
                            : migrationSettings.SharePointDocumentLibrary ?? string.Empty;
                        var catchDestPath = StripLibraryPrefix(record.MappedPath, catchLibrary);
                        var failed = new BlobToSharePointMigrator.Models.MigrationResult
                        {
                            SourceFile = record.BlobPath,
                            DestPath = catchDestPath,
                            SizeBytes = record.SizeBytes,
                            LastModified = record.LastModified,
                            Status = "Failed",
                            SharePointUrl = $"{migrationSettings.SharePointSiteUrl.TrimEnd('/')}/{catchLibrary}/{catchDestPath}",
                            Error = ex.Message,
                            Duration = "N/A (batch operation)"
                        };

                        lock (allResults)
                        {
                            allResults.Add(failed);
                        }
                    }
                }
                finally
                {
                    libSem.Release();
                    gate.Release();
                }
            }));
        }

        await Task.WhenAll(tasks);
    }

    // ── STEP 5/5: Bulk CSOM metadata patch (CaseId / CaseType / DocumentId) ───────────────────────
    logger.LogInformation("STEP 5/5 - Bulk CSOM metadata patch (CaseId / CaseType / DocumentId)...");
    {
        var successBlobPaths = new HashSet<string>(allResults
            .Where(r => r.Status is "Success" or "PartialSuccess")
            .Select(r => r.SourceFile), StringComparer.OrdinalIgnoreCase);
        var metaRecords = toMigrate.Where(r => successBlobPaths.Contains(r.BlobPath)).ToList();

        if (metaRecords.Count == 0)
        {
            logger.LogInformation("STEP 5/5 - No successful uploads to patch.");
        }
        else
        {
            await PatchCaseMetadataForExistingFilesAsync(metaRecords, "STEP 5/5");
        }
    }

    var lastResultBySource = allResults
        .GroupBy(r => r.SourceFile, StringComparer.OrdinalIgnoreCase)
        .ToDictionary(g => g.Key, g => g.Last(), StringComparer.OrdinalIgnoreCase);
    var overwriteAuditRows = toMigrate
        .Select(record =>
        {
            lastResultBySource.TryGetValue(record.BlobPath, out var result);
            batchAuditBySourceFile.TryGetValue(record.BlobPath, out var audit);
            var caseFolder = ExtractCaseFolderFromMappedPath(record.MappedPath);
            var year = caseFolder.Split('/', StringSplitOptions.RemoveEmptyEntries).FirstOrDefault() ?? string.Empty;
            var documentIdPresent = record.Metadata.TryGetValue("DocumentId", out var did) && !string.IsNullOrWhiteSpace(did);
            var metadataPatchEligible = result?.Status is "Success" or "PartialSuccess";
            var note = audit.Note;
            if (!documentIdPresent)
                note = string.IsNullOrWhiteSpace(note) ? "DocumentId missing after manifest matching." : $"{note} DocumentId missing after manifest matching.";

            return new OverwriteAuditRow
            {
                SourceFile = record.BlobPath,
                DestPath = result?.DestPath ?? record.MappedPath,
                CaseFolder = caseFolder,
                Year = year,
                Status = result?.Status ?? "NotSubmitted",
                DocumentIdPresent = documentIdPresent,
                MetadataPatchEligible = metadataPatchEligible,
                BatchAlreadyExistsSignal = audit.AlreadyExistsSignal,
                BatchJobStatus = audit.JobStatus ?? string.Empty,
                BatchFilesCreated = audit.FilesCreated,
                BatchSubmittedCount = audit.SubmittedCount,
                Notes = note
            };
        })
        .OrderBy(r => r.CaseFolder, StringComparer.OrdinalIgnoreCase)
        .ThenBy(r => r.DestPath, StringComparer.OrdinalIgnoreCase)
        .ToList();

    reportSvc.SaveDeltaTracking();
    reportSvc.WriteReport(allResults);
    reportSvc.WriteOverwriteAuditReport(overwriteAuditRows);
    reportSvc.WriteFailedItems(allResults);
    // Recompute estimated case-folder count for summary
    var estimatedCaseFolders = 0;
    if (migrationSettings.UseYyyyCaseNumberPath)
    {
        var cf = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var r in toMigrate)
        {
            var path = (r.MappedPath ?? string.Empty).Replace('\\', '/').Trim('/');
            var segs = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
            if (segs.Length >= 2 && System.Text.RegularExpressions.Regex.IsMatch(segs[0], "^\\d{4}$") && System.Text.RegularExpressions.Regex.IsMatch(segs[1], "^\\d+$"))
                cf.Add($"{segs[0]}/{segs[1]}");
        }
        estimatedCaseFolders = cf.Count;
    }
    reportSvc.PrintSummary(
        allResults,
        skipped,
        aggregateAlreadyExists,
        records.Count,
        toMigrate.Count,
        estimatedCaseFolders,
        aggregateOtherErrors,
        migrationSettings.ReportExistingFilesAsOverwritten,
        BuildSummaryYearLabel(migrationSettings, toMigrate));

    logger.LogInformation(string.Empty);
    logger.LogInformation("Migration complete! Submitted jobs: {Jobs}, Total files: {Total}", batchesToRun.Count, toMigrate.Count);
    logger.LogInformation("Conflict summary: AlreadyExists={AlreadyExists}, OtherErrors={OtherErrors}",
        aggregateAlreadyExists, aggregateOtherErrors);
}
catch (Exception ex)
{
    logger.LogCritical(ex, "Pipeline failed.");
    Console.WriteLine($"\nFatal error: {ex.Message}");
    Console.WriteLine(ex.ToString());
    Environment.Exit(1);
}
finally
{
    Log.CloseAndFlush();
}
