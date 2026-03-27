using BlobToSharePointMigrator.Configuration;
using BlobToSharePointMigrator.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Serilog;

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

    if (toMigrate.Count == 0)
    {
        logger.LogInformation("No files to migrate (all already migrated or filtered)");
        reportSvc.SaveDeltaTracking();
        reportSvc.PrintSummary(new List<BlobToSharePointMigrator.Models.MigrationResult>(), skipped);
        Environment.Exit(0);
    }

    logger.LogInformation("STEP 3/5 - Connecting to SharePoint...");
    await spService.ConnectAsync();

    logger.LogInformation("STEP 4/5 - Submitting migration job for {Count} files via SharePoint Migration API...", toMigrate.Count);
    var jobInfo = await spService.SubmitMigrationJobAsync(toMigrate, blobService.DownloadBlobAsync);

    logger.LogInformation("Migration job submitted: {JobId}", jobInfo.JobId);

    logger.LogInformation("STEP 5/5 - Polling migration job status (this may take several minutes for large batches)...");
    var pollIntervalSeconds = Math.Max(1, migrationSettings.JobPollIntervalSeconds);
    var timeoutMinutes = Math.Max(1, migrationSettings.JobTimeoutMinutes);
    var finalJobInfo = await spService.PollMigrationJobAsync(
        jobInfo.JobId,
        TimeSpan.FromMinutes(timeoutMinutes),
        pollIntervalSeconds * 1000);

    await spService.CleanupStagingContainersAsync();

    var results = new List<BlobToSharePointMigrator.Models.MigrationResult>();
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

    foreach (var record in toMigrate)
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
        results.Add(result);

        if (result.Status == "Success" || result.Status == "PartialSuccess")
            reportSvc.TrackMigrated(record);
    }

    reportSvc.SaveDeltaTracking();
    reportSvc.WriteReport(results);
    reportSvc.PrintSummary(results, skipped);

    logger.LogInformation(string.Empty);
    logger.LogInformation("Migration complete!");
    logger.LogInformation("Job Status: {Status}", finalJobInfo.Status);
    logger.LogInformation("Files Processed: {Processed}/{Total}",
        finalJobInfo.ProcessedFileCount,
        finalJobInfo.TotalFileCount);
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
