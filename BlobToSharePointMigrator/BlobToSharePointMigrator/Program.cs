using BlobToSharePointMigrator.Configuration;
using BlobToSharePointMigrator.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Serilog;
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
    migrationSection = config.GetSection("SimpleETL");
if (!migrationSection.Exists())
    throw new InvalidOperationException("Neither 'Migration' nor 'SimpleETL' section was found in appsettings.json");

var settings = config.GetSection("SimpleETL");
if (!settings.Exists())
    settings = migrationSection;

var migrationSettings = migrationSection.Get<MigrationSettings>()
    ?? throw new InvalidOperationException("Migration settings could not be bound from 'Migration' or 'SimpleETL' section.");

var services = new ServiceCollection()
    .AddLogging(b => b
        .ClearProviders()
        .AddSerilog(Log.Logger, dispose: false)
        .SetMinimumLevel(LogLevel.Information))
    .BuildServiceProvider();

var loggerFactory = services.GetRequiredService<ILoggerFactory>();

var blobService = new BlobInventoryService(settings, migrationSettings, loggerFactory.CreateLogger<BlobInventoryService>());
var transformSvc = new PathTransformService(
    Path.Combine(AppContext.BaseDirectory, migrationSettings.MappingFile),
    migrationSettings.UseYyyyCaseNumberPath,
    loggerFactory.CreateLogger<PathTransformService>());
var spService = new SharePointMigrationService(settings, migrationSettings, loggerFactory.CreateLogger<SharePointMigrationService>());
var reportSvc = new ReportService(migrationSettings, loggerFactory.CreateLogger<ReportService>());

var logger = loggerFactory.CreateLogger("Pipeline");

try
{
    reportSvc.LoadDeltaTracking();

    logger.LogInformation("STEP 1/5 - Inventorying Azure Blob Storage...");
    var records = await blobService.InventoryAsync();

    logger.LogInformation("STEP 2/5 - Transforming folder paths...");
    transformSvc.TransformAll(records);

    var allowed = records.Where(r => r.IsAllowed).ToList();
    var skipped = records.Where(r => !r.IsAllowed).ToList();

    foreach (var s in skipped)
        logger.LogInformation("Skipped: {File} ({Reason})", s.BlobPath, s.SkipReason);

    var toMigrate = allowed.Where(r => !reportSvc.ShouldSkip(r)).ToList();
    if (migrationSettings.MaxFilesToMigrate > 0 && toMigrate.Count > migrationSettings.MaxFilesToMigrate)
    {
        toMigrate = toMigrate.Take(migrationSettings.MaxFilesToMigrate).ToList();
        logger.LogInformation("Milestone cap active: limiting run to first {Limit} files.", migrationSettings.MaxFilesToMigrate);
    }

    logger.LogInformation("Files to migrate (after delta): {Count} of {Total}", toMigrate.Count, allowed.Count);

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
        logger.LogInformation("No files to migrate (all already migrated or filtered)");
        reportSvc.SaveDeltaTracking();
        reportSvc.PrintSummary(new List<BlobToSharePointMigrator.Models.MigrationResult>(), skipped);
        Environment.Exit(0);
    }

    logger.LogInformation("STEP 3/5 - Connecting to SharePoint...");
    await spService.ConnectAsync();

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

    var allResults = new List<BlobToSharePointMigrator.Models.MigrationResult>();

    // Run with limited parallelism based on config
    var parallelJobs = Math.Max(1, migrationSettings.MaxParallelJobs);
    using (var gate = new System.Threading.SemaphoreSlim(parallelJobs))
    {
        var tasks = new List<Task>();
        for (int i = 0; i < batchesToRun.Count; i++)
        {
            var index = i;
            var batch = batchesToRun[i];
            await gate.WaitAsync();
            tasks.Add(Task.Run(async () =>
            {
                try
                {
                    logger.LogInformation("Submitting job {Index}/{TotalJobs} for {Count} files...",
                        index + 1, batchesToRun.Count, batch.Count);
                    var jobInfo = await spService.SubmitMigrationJobAsync(batch, blobService.DownloadBlobAsync);
                    logger.LogInformation("Migration job submitted: {JobId}", jobInfo.JobId);

                    var pollIntervalSeconds = Math.Max(1, migrationSettings.JobPollIntervalSeconds);
                    var timeoutMinutes = Math.Max(1, migrationSettings.JobTimeoutMinutes);
                    var finalJobInfo = await spService.PollMigrationJobAsync(
                        jobInfo.JobId,
                        TimeSpan.FromMinutes(timeoutMinutes),
                        pollIntervalSeconds * 1000);

                    var markAllFailed = finalJobInfo.Status == "Failed" ||
                                        (finalJobInfo.Status == "CompletedWithErrors" && finalJobInfo.ProcessedFileCount == 0);
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
                        var rowStatus = markAllFailed
                            ? "Failed"
                            : finalJobInfo.Status == "Completed"
                                ? "Success"
                                : finalJobInfo.Status == "CompletedWithErrors"
                                    ? "PartialSuccess"
                                    : "Failed";

                        var result = new BlobToSharePointMigrator.Models.MigrationResult
                        {
                            SourceFile = record.BlobPath,
                            DestPath = record.MappedPath,
                            SizeBytes = record.SizeBytes,
                            LastModified = record.LastModified,
                            Status = rowStatus,
                            SharePointUrl = $"{migrationSettings.SharePointSiteUrl.TrimEnd('/')}/{migrationSettings.SharePointDocumentLibrary}/{record.MappedPath}",
                            Error = rowStatus == "Failed" ? firstError : string.Empty,
                            Duration = "N/A (batch operation)"
                        };

                        lock (allResults)
                        {
                            allResults.Add(result);
                        }

                        if (result.Status == "Success" || result.Status == "PartialSuccess")
                            reportSvc.TrackMigrated(record);
                    }

                    logger.LogInformation("Job {Index}/{TotalJobs} complete: Status={Status}, Processed={Processed}/{Total}",
                        index + 1, batchesToRun.Count, finalJobInfo.Status, finalJobInfo.ProcessedFileCount, finalJobInfo.TotalFileCount);
                }
                finally
                {
                    gate.Release();
                }
            }));
        }

        await Task.WhenAll(tasks);
    }

    await spService.CleanupStagingContainersAsync();

    reportSvc.SaveDeltaTracking();
    reportSvc.WriteReport(allResults);
    reportSvc.PrintSummary(allResults, skipped);

    logger.LogInformation(string.Empty);
    logger.LogInformation("Migration complete! Submitted jobs: {Jobs}, Total files: {Total}", batchesToRun.Count, toMigrate.Count);
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
