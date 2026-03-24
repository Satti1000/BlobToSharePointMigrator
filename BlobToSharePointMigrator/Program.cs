using BlobToSharePointMigrator.Configuration;
using BlobToSharePointMigrator.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

Console.WriteLine();
Console.WriteLine("═══════════════════════════════════════════════════");
Console.WriteLine("   Blob-to-SharePoint ETL Migration Pipeline");
Console.WriteLine("   .NET 8 | SharePoint Migration API");
Console.WriteLine("═══════════════════════════════════════════════════");
Console.WriteLine();

// ── Configuration ────────────────────────────────────────
var config = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: false)
    .AddEnvironmentVariables()
    .Build();

var settings = config.GetSection("Migration").Get<MigrationSettings>()
    ?? throw new InvalidOperationException("Migration settings not found in appsettings.json");

// ── Dependency Injection ─────────────────────────────────
var services = new ServiceCollection()
	.AddLogging(b => b
		.AddConsole()
		.SetMinimumLevel(LogLevel.Information))
    .BuildServiceProvider();

var loggerFactory = services.GetRequiredService<ILoggerFactory>();

// ── Services ─────────────────────────────────────────────
var blobService  = new BlobInventoryService(settings, loggerFactory.CreateLogger<BlobInventoryService>());
var transformSvc = new PathTransformService(Path.Combine(AppContext.BaseDirectory, settings.MappingFile), loggerFactory.CreateLogger<PathTransformService>());
var spService    = new SharePointMigrationService(settings, loggerFactory.CreateLogger<SharePointMigrationService>());
var reportSvc    = new ReportService(settings, loggerFactory.CreateLogger<ReportService>());

var logger = loggerFactory.CreateLogger("Pipeline");

try
{
    // ── Step 1: Load delta tracking ───────────────────────
    reportSvc.LoadDeltaTracking();

    // ── Step 2: Inventory blobs ───────────────────────────
    logger.LogInformation("STEP 1/5 — Inventorying Azure Blob Storage...");
    var records = await blobService.InventoryAsync();

    // ── Step 3: Transform paths ───────────────────────────
    logger.LogInformation("STEP 2/5 — Transforming folder paths...");
    transformSvc.TransformAll(records);

    var allowed = records.Where(r => r.IsAllowed).ToList();
    var skipped = records.Where(r => !r.IsAllowed).ToList();

    foreach (var s in skipped)
        logger.LogInformation("Skipped: {File} ({Reason})", s.BlobPath, s.SkipReason);

    // Apply delta filtering
    var toMigrate = allowed.Where(r => !reportSvc.ShouldSkip(r)).ToList();
    logger.LogInformation("Files to migrate (after delta): {Count} of {Total}", toMigrate.Count, allowed.Count);

    if (toMigrate.Count == 0)
    {
        logger.LogInformation("No files to migrate (all already migrated or filtered)");
        reportSvc.SaveDeltaTracking();
        reportSvc.PrintSummary(new List<BlobToSharePointMigrator.Models.MigrationResult>(), skipped);
        Environment.Exit(0);
    }

    // ── Step 4: Connect to SharePoint ─────────────────────
    logger.LogInformation("STEP 3/5 — Connecting to SharePoint...");
    await spService.ConnectAsync();

    // ── Step 5: Submit migration job ─────────────────────
    logger.LogInformation("STEP 4/5 — Submitting migration job for {Count} files via SharePoint Migration API...", toMigrate.Count);

    var jobInfo = await spService.SubmitMigrationJobAsync(
        toMigrate,
        blobService.DownloadBlobAsync);

    logger.LogInformation("Migration job submitted: {JobId}", jobInfo.JobId);

    // ── Step 6: Poll for completion ──────────────────────
    logger.LogInformation("STEP 5/5 — Polling migration job status (this may take several minutes for large batches)...");
    var finalJobInfo = await spService.PollMigrationJobAsync(jobInfo.JobId);

    // ── Step 6b: Cleanup staging containers ──────────────
    await spService.CleanupStagingContainersAsync();

    // Build results from job status
    var results = new List<BlobToSharePointMigrator.Models.MigrationResult>();
    foreach (var record in toMigrate)
    {
        var result = new BlobToSharePointMigrator.Models.MigrationResult
        {
            SourceFile = record.BlobPath,
            DestPath = record.MappedPath,
            SizeBytes = record.SizeBytes,
            LastModified = record.LastModified,
            Status = finalJobInfo.Status == "Completed" ? "Success" :
                     finalJobInfo.Status == "CompletedWithErrors" ? "PartialSuccess" : "Failed",
            SharePointUrl = $"{settings.SharePointSiteUrl.TrimEnd('/')}/{settings.SharePointDocumentLibrary}/{record.MappedPath}",
            Duration = "N/A (batch operation)"
        };
        results.Add(result);

        if (result.Status == "Success" || result.Status == "PartialSuccess")
            reportSvc.TrackMigrated(record);
    }

    // ── Step 7: Save report & tracking ───────────────────
    reportSvc.SaveDeltaTracking();
    reportSvc.WriteReport(results);
    reportSvc.PrintSummary(results, skipped);

    logger.LogInformation("");
    logger.LogInformation("Migration complete!");
    logger.LogInformation("Job Status: {Status}", finalJobInfo.Status);
    logger.LogInformation("Files Processed: {Processed}/{Total}",
        finalJobInfo.ProcessedFileCount,
        finalJobInfo.TotalFileCount);
}
catch (Exception ex)
{
    logger.LogCritical("Pipeline failed: {Error}", ex.Message);
    Console.WriteLine($"\n✗ Fatal error: {ex.Message}");
    Environment.Exit(1);
}
