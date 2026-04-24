using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using BlobToSharePointMigrator.Configuration;
using BlobToSharePointMigrator.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Identity.Client;
using Microsoft.SharePoint.Client;
using PnP.Framework;
using System.Diagnostics;
using System.Net.Http;
using System.Net;
using System.Security;
using System.Security.Cryptography.X509Certificates;
using System.Xml.Linq;
using System.Linq;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using SpFile = Microsoft.SharePoint.Client.File;

namespace BlobToSharePointMigrator.Services;

/// <summary>
/// Migrates files to SharePoint using the SharePoint Migration API (CreateMigrationJob via CSOM).
///
/// Flow:
/// 1. Authenticate via certificate-based OAuth (JWT client assertion → access token)
/// 2. Upload source files to a temp Azure Blob container
/// 3. Generate SPMI XML manifest package (8 XML files)
/// 4. Upload manifest XMLs to a separate Azure Blob container
/// 5. Call Site.CreateMigrationJob with SAS URLs for both containers
/// 6. Poll Site.GetMigrationJobStatus until completion
/// </summary>
public class SharePointMigrationService
{
    private readonly MigrationSettings _settings;
    private IConfigurationSection _processFlags;
    private readonly ILogger<SharePointMigrationService> _logger;
    private ClientContext _clientContextG = null!;
    private Site _site = null!;
    private Web _web = null!;
    private string _siteId = string.Empty;
    private string _webId = string.Empty;
    private string _listId = string.Empty;
    private string _rootFolderId = string.Empty;
    private string _rootFolderUrl = string.Empty;
    private string _queueUri = string.Empty;
    private byte[] _encryptionKey = Array.Empty<byte>();
    private readonly Dictionary<string, string> _effectiveMetadataFieldMap;
    private readonly Dictionary<string, Dictionary<string, string>> _resolvedMetadataFieldMapByLibrary = new(StringComparer.OrdinalIgnoreCase);
    // Progress log deduplication for queue polling
    private int _lastQueueFilesCreated = -1;
    private int _lastQueueErrors = -1;
    private DateTime _lastQueueProgressLogUtc = DateTime.MinValue;

    public SharePointMigrationService(IConfigurationSection processFlags, MigrationSettings settings, ILogger<SharePointMigrationService> logger)
    {
        _settings = settings;
        _logger = logger;
        _processFlags = processFlags;
        _effectiveMetadataFieldMap = new Dictionary<string, string>(_settings.MetadataFieldMap, StringComparer.OrdinalIgnoreCase);
    }

    private string GetTenantName()
    {
        var url = _processFlags.GetSection("AdminUrl").Value;
        if (string.IsNullOrWhiteSpace(url))
            throw new InvalidOperationException("AdminUrl is not configured under SimpleETL:AdminUrl.");
        return new Uri(url).Host.Split('.')[0];
    }

    /// <summary>
    /// Authenticates to SharePoint using certificate-based auth via PnP.Framework,
    /// then loads Site/Web/List IDs needed for manifest generation.
    /// </summary>
    /// <returns></returns>
    public async Task ConnectAsync()
    {
        try
        {
            // Prefer certificate-based app-only auth for CSOM + SPMI (matches README + avoids scope/audience mistakes).
            var siteUrl = _processFlags.GetSection("SHAREPOINT_SITE_URL").Value?.Trim();
            if (string.IsNullOrWhiteSpace(siteUrl))
                siteUrl = _settings.SharePointSiteUrl?.Trim();

            if (string.IsNullOrWhiteSpace(siteUrl))
                throw new InvalidOperationException("SharePoint site url not configured. Set SimpleETL:SHAREPOINT_SITE_URL or Migration:SharePointSiteUrl.");

            _logger.LogInformation("Connecting to SharePoint (PnP.Framework CSOM app-only): {Url}", siteUrl);

            var certificate = LoadCertificate(_settings);
            var authManager = new PnP.Framework.AuthenticationManager(
                _settings.SharePointClientId,
                certificate,
                _settings.SharePointTenantId);

            // IMPORTANT: do NOT wrap this in a using; we need it for the rest of the pipeline.
            _clientContextG = await authManager.GetContextAsync(siteUrl).ConfigureAwait(false);
            _clientContextG.RequestTimeout = Math.Max(60, _settings.CsomRequestTimeoutSeconds) * 1000;

            // Load site, web, and target document library metadata
            _site = _clientContextG.Site;
            _web = _clientContextG.Web;
            _clientContextG.Load(_site, s => s.Id, s => s.Url);
            _clientContextG.Load(_web, w => w.Id, w => w.Title, w => w.ServerRelativeUrl);
            await ExecuteQueryWithRetryAsync().ConfigureAwait(false);

            _siteId = _site.Id.ToString();
            _webId = _web.Id.ToString();

            _logger.LogInformation("Connected — Site ID: {SiteId}, Web ID: {WebId}, Title: {Title}", _siteId, _webId, _web.Title);

            // Do NOT resolve a specific library here; that will be done per job,
            // enabling Year-as-Library mode.
            _logger.LogInformation("Connected to site/web. Library will be resolved per job.");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error connecting to SharePoint");
            throw;
        }

    }

    private async Task ResolveTargetLibraryAsync(string libraryTitle)
    {
        var documentLibraryTitle = (libraryTitle ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(documentLibraryTitle))
        {
            throw new InvalidOperationException(
                "Target library title is empty. Provide a valid SharePoint document library title.");
        }

        try
        {
            var list = _web.Lists.GetByTitle(documentLibraryTitle);
            _clientContextG.Load(list, l => l.Id, l => l.RootFolder);
            _clientContextG.Load(list.RootFolder, f => f.UniqueId, f => f.ServerRelativeUrl);
            await ExecuteQueryWithRetryAsync().ConfigureAwait(false);

            _listId = list.Id.ToString();
            _rootFolderId = list.RootFolder.UniqueId.ToString();

            var listRootServerRelative = list.RootFolder.ServerRelativeUrl.Trim('/');
            var webServerRelative = _web.ServerRelativeUrl.Trim('/');
            if (!string.IsNullOrWhiteSpace(webServerRelative) &&
                listRootServerRelative.StartsWith(webServerRelative + "/", StringComparison.OrdinalIgnoreCase))
            {
                _rootFolderUrl = listRootServerRelative[(webServerRelative.Length + 1)..];
            }
            else
            {
                _rootFolderUrl = listRootServerRelative;
            }

            _logger.LogInformation("Resolved target library: {Library} (List ID: {ListId}, Root URL: {RootUrl})",
                documentLibraryTitle, _listId, _rootFolderUrl);
        }
        catch (ServerException ex) when (
            ex.Message.Contains("title", StringComparison.OrdinalIgnoreCase) &&
            ex.Message.Contains("invalid", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"Invalid library title '{documentLibraryTitle}'. Use the exact library title from SharePoint Library Settings (for example: 'Documents', 'Shared Documents', or '2014').",
                ex);
        }
        catch (ServerException ex) when (
            ex.Message.Contains("does not exist", StringComparison.OrdinalIgnoreCase) ||
            ex.Message.Contains("not found", StringComparison.OrdinalIgnoreCase) ||
            ex.ServerErrorCode == -2130575338)   // 0x81020026 = list not found
        {
            throw new InvalidOperationException(
                $"SharePoint library '{documentLibraryTitle}' was not found on {_settings.SharePointSiteUrl}. " +
                "Create the library first, or check the YearAsLibrary setting and the year being migrated.",
                ex);
        }
        catch (ServerException ex)
        {
            throw new InvalidOperationException(
                $"CSOM error resolving library '{documentLibraryTitle}': {ex.Message}", ex);
        }
    }

    private static X509Certificate2 LoadCertificate(MigrationSettings settings)
    {
        if (!string.IsNullOrWhiteSpace(settings.SharePointCertificatePath))
        {
            return new X509Certificate2(
                settings.SharePointCertificatePath,
                settings.SharePointCertificatePassword,
                X509KeyStorageFlags.EphemeralKeySet);
        }

        if (!string.IsNullOrWhiteSpace(settings.SharePointCertificateThumbprint))
        {
            using var store = new X509Store(StoreName.My, StoreLocation.CurrentUser);
            store.Open(OpenFlags.ReadOnly);
            var matches = store.Certificates.Find(
                X509FindType.FindByThumbprint,
                settings.SharePointCertificateThumbprint.Replace(" ", string.Empty),
                validOnly: false);

            if (matches.Count > 0)
                return matches[0];
        }

        throw new InvalidOperationException(
            "Certificate not configured. Provide Migration:SharePointCertificatePath (+Password) or Migration:SharePointCertificateThumbprint.");
    }
     

    /// <summary>
    /// Runs the full SPMI migration pipeline:
    /// 1. Provision SharePoint-managed Azure containers (guaranteed compatible SAS)
    /// 2. Upload source files to the data container
    /// 3. Generate and upload manifest XMLs to the metadata container
    /// 4. Call CreateMigrationJob with the Web ID and container URIs
    /// </summary>
    public async Task<MigrationJobInfo> SubmitMigrationJobAsync(
        List<FileRecord> records,
        Func<string, Task<Stream>> blobDownloader,
        string? libraryTitleOverride = null)
    {
        if (records.Count == 0)
            throw new ArgumentException("No files to migrate");

        _logger.LogInformation("Preparing SPMI migration package for {Count} files...", records.Count);

        // Resolve the target library for this job
        var targetLibraryTitle = (libraryTitleOverride ?? _settings.SharePointDocumentLibrary)?.Trim() ?? string.Empty;
        await ResolveTargetLibraryAsync(targetLibraryTitle);

        // Pre-validate and normalize mapped paths. Bad paths are skipped so one invalid item
        // does not fail the full migration batch. Sanitize first, then validate.
        var validRecords = new List<FileRecord>(records.Count);
        var skippedInvalidPath = 0;
        foreach (var record in records)
        {
            try
            {
                var rawPath = (record.MappedPath ?? string.Empty).Replace('\\', '/').Trim('/');
                if (string.IsNullOrWhiteSpace(rawPath))
                    throw new InvalidOperationException("Mapped path is empty.");

                // Sanitize first so any residual invalid chars from intermediate transforms are handled.
                var safePath = PathTransformService.SanitizeSharePointRelativePath(rawPath);
                if (string.IsNullOrWhiteSpace(safePath))
                    throw new InvalidOperationException("Mapped path became empty after sanitization.");

                if (safePath.Length > 400)
                {
                    var originalLength = safePath.Length;
                    safePath = PathTransformService.TruncateSharePointRelativePath(safePath, 400);
                    _logger.LogInformation(
                        "Truncated long mapped path from {OriginalLength} to {NewLength} chars for blob {BlobPath}.",
                        originalLength, safePath.Length, record.BlobPath);
                }

                record.MappedPath = safePath;
                validRecords.Add(record);
            }
            catch (Exception ex)
            {
                skippedInvalidPath++;
                _logger.LogWarning("Skipping item due to invalid target path. Blob: {BlobPath}, MappedPath: {MappedPath}, Reason: {Reason}",
                    record.BlobPath, record.MappedPath, ex.Message);
            }
        }

        if (validRecords.Count == 0)
            throw new InvalidOperationException("All files were skipped during path validation. No valid files to migrate.");

        if (skippedInvalidPath > 0)
            _logger.LogWarning("Skipped {SkippedCount} item(s) due to invalid/special-character paths. Continuing with {ValidCount} item(s).",
                skippedInvalidPath, validRecords.Count);

        await EnsureMetadataFieldMappingsAsync(validRecords, targetLibraryTitle).ConfigureAwait(false);

        // Step 1: Provision SharePoint-managed migration containers + queue
        _logger.LogInformation("Provisioning SharePoint migration containers and queue...");

        string? dataContainerUri = null;
        string? metadataContainerUri = null;
        string? queueUri = null;
        bool provisioned = false;

        for (int attempt = 1; attempt <= 3 && !provisioned; attempt++)
        {
            try
            {
                var containersResult = _site.ProvisionMigrationContainers();
                var queueResult = _site.ProvisionMigrationQueue();
                await ExecuteQueryWithRetryAsync();

                var containers = containersResult?.Value;
                dataContainerUri = containers?.DataContainerUri;
                metadataContainerUri = containers?.MetadataContainerUri;
                queueUri = queueResult?.Value?.JobQueueUri;
                _encryptionKey = containers?.EncryptionKey ?? Array.Empty<byte>();

                provisioned = !string.IsNullOrWhiteSpace(dataContainerUri)
                              && !string.IsNullOrWhiteSpace(metadataContainerUri)
                              && !string.IsNullOrWhiteSpace(queueUri)
                              && _encryptionKey.Length > 0;

                if (!provisioned)
                {
                    _logger.LogWarning("Provision attempt {Attempt}/3 did not return valid URIs. Retrying in 2s...", attempt);
                    await Task.Delay(TimeSpan.FromSeconds(2));
                }
            }
            catch (Exception ex) when (IsTransientRequestError(ex))
            {
                _logger.LogWarning(ex, "Transient error provisioning migration containers (attempt {Attempt}/3). Retrying in 2s...", attempt);
                await Task.Delay(TimeSpan.FromSeconds(2));
            }
        }

        if (!provisioned)
            throw new InvalidOperationException("Failed to provision SharePoint migration containers/queue. Verify site permissions and try again.");

        // By this point values are validated; use non-null locals for downstream usage.
        var dataContainerUriValue = dataContainerUri!;
        var metadataContainerUriValue = metadataContainerUri!;
        var queueUriValue = queueUri!;

        _logger.LogInformation("Data container provisioned: {Uri}", dataContainerUriValue.Split('?')[0]);
        _logger.LogInformation("Metadata container provisioned: {Uri}", metadataContainerUriValue.Split('?')[0]);
        _queueUri = queueUriValue;
        _logger.LogInformation("Report queue provisioned.");

        // Step 2: Upload source files to the data container (AES-encrypted)
        _logger.LogInformation("Uploading {Count} source files (encrypted) to SharePoint data container...", validRecords.Count);

        var dataContainer = new BlobContainerClient(new Uri(dataContainerUriValue));
        var uploadParallelism = Math.Max(1, _settings.UploadParallelism);
        _logger.LogInformation("Data upload parallelism: {Parallelism}", uploadParallelism);
        var uploadedCount = 0;
        var uploadSw = Stopwatch.StartNew();
        var totalToUpload = validRecords.Count;

        await Parallel.ForEachAsync(
            validRecords,
            new ParallelOptions { MaxDegreeOfParallelism = uploadParallelism },
            async (record, _) =>
            {
                var targetPath = record.MappedPath.Replace('\\', '/').TrimStart('/');
                await using var stream = await blobDownloader(record.BlobPath);
                await UploadEncryptedBlobAsync(dataContainer, targetPath, stream);

                if (_settings.LogPerFileCaseProgress)
                {
                    record.Metadata.TryGetValue("CaseId", out var caseId);
                    record.Metadata.TryGetValue("CaseType", out var caseType);
                    _logger.LogInformation(
                        "File packaged for SharePoint import: CaseId={CaseId} CaseType={CaseType} File={FileName} TargetPath={TargetPath} (encrypted staging complete; library write follows via migration job)",
                        string.IsNullOrWhiteSpace(caseId) ? "—" : caseId,
                        string.IsNullOrWhiteSpace(caseType) ? "—" : caseType,
                        record.Name,
                        targetPath);
                }

                var finished = Interlocked.Increment(ref uploadedCount);
                if (finished % 250 == 0 || finished == validRecords.Count)
                {
                    var elapsed = uploadSw.Elapsed.TotalSeconds;
                    var rate = elapsed > 0 ? finished / elapsed : 0;
                    var remaining = Math.Max(0, totalToUpload - finished);
                    var etaSeconds = rate > 0 ? remaining / rate : double.NaN;
                    var eta = TimeSpan.FromSeconds(double.IsNaN(etaSeconds) ? 0 : etaSeconds);
                    _logger.LogInformation("Data upload progress: {Uploaded}/{Total} | {Rate}/s | ETA {Eta}",
                        finished, totalToUpload, rate.ToString("0.0"), eta.ToString(@"mm\:ss"));
                }
            });

        _logger.LogInformation("All source files uploaded. Duration: {Duration}s, Avg rate: {Rate}/s",
            uploadSw.Elapsed.TotalSeconds.ToString("0.0"),
            (totalToUpload / Math.Max(1, uploadSw.Elapsed.TotalSeconds)).ToString("0.0"));

        // Step 3: Generate manifest XMLs
        var webUrl = _web.ServerRelativeUrl.TrimEnd('/');
        var manifests = GenerateManifestPackage(validRecords, _web.Id, webUrl);

        // Step 4: Upload manifest XMLs to the metadata container (AES-encrypted)
        _logger.LogInformation("Uploading manifest package ({Count} encrypted XML files)...", manifests.Count);

        var metadataContainer = new BlobContainerClient(new Uri(metadataContainerUriValue));

        foreach (var (fileName, content) in manifests)
        {
            using var ms = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(content));
            await UploadEncryptedBlobAsync(metadataContainer, fileName, ms);
        }

        _logger.LogInformation("Manifest package uploaded.");

        // Step 5: Call CreateMigrationJobEncrypted via CSOM
        // Provisioned containers require encryption — pass the key from ProvisionMigrationContainers
        _logger.LogInformation("Calling CreateMigrationJobEncrypted (Web ID: {WebId})...", _web.Id);

        var encryptionOption = new EncryptionOption { AES256CBCKey = _encryptionKey };

        var jobIdResult = _site.CreateMigrationJobEncrypted(
            _web.Id,
            dataContainerUriValue,
            metadataContainerUriValue,
            queueUriValue,
            encryptionOption);

        await ExecuteQueryWithRetryAsync();

        var jobId = jobIdResult.Value;
        _logger.LogInformation("Migration job submitted: {JobId}", jobId);

        return new MigrationJobInfo
        {
            JobId = jobId,
            Status = "Queued",
            Progress = 0,
            CreatedDateTime = DateTime.UtcNow.ToString("O"),
            TotalFileCount = validRecords.Count
        };
    }

    /// <summary>
    /// Polls GetMigrationJobStatus until the job completes or times out.
    /// MigrationJobState only has: None(0), Queued(2), Processing(4).
    /// When a job finishes (success or failure), it returns None.
    /// We detect completion by seeing None AFTER the job was Queued/Processing.
    /// </summary>
    public async Task<MigrationJobInfo> PollMigrationJobAsync(
        Guid jobId,
        TimeSpan? timeout = null,
        int pollIntervalMs = 10000,
        int expectedFileCount = 0)
    {
        timeout ??= TimeSpan.FromMinutes(60);
        var sw = Stopwatch.StartNew();
        bool wasActive = false;

        _logger.LogInformation("Polling migration job {JobId} (timeout: {Timeout}min)...",
            jobId, timeout.Value.TotalMinutes);

        var lastElapsedLog = TimeSpan.Zero;
        while (sw.Elapsed < timeout)
        {
            ClientResult<MigrationJobState>? statusResult = null;
            try
            {
                statusResult = _site.GetMigrationJobStatus(jobId);
                await ExecuteQueryWithRetryAsync();
            }
            catch (Exception ex) when (IsTransientRequestError(ex))
            {
                _logger.LogWarning(ex, "Transient polling error for job {JobId}; continuing.", jobId);
                await Task.Delay(pollIntervalMs);
                continue;
            }

            var state = statusResult!.Value;
            _logger.LogInformation("Job {JobId} — State: {State}", jobId, state);

            // Emit periodic elapsed time markers so the console shows forward progress even if
            // the state remains Processing for a long time on SharePoint side.
            var elapsed = sw.Elapsed;
            if (elapsed - lastElapsedLog >= TimeSpan.FromMinutes(1))
            {
                lastElapsedLog = elapsed;
                _logger.LogInformation("Polling {JobId} — Elapsed: {Elapsed} (timeout at {Timeout} min)",
                    jobId, $"{(int)elapsed.TotalMinutes}m {elapsed.Seconds:D2}s", (int)timeout.Value.TotalMinutes);
            }

            if (state is MigrationJobState.Queued or MigrationJobState.Processing)
            {
                wasActive = true;
            }
            else if (state == MigrationJobState.None)
            {
                // SharePoint sometimes returns None without a clear transition. We use the migration queue
                // to determine whether we reached a final event (JobEnd/JobError).
                _logger.LogInformation("Job {JobId} returned State=None.", jobId);
                _logger.LogInformation("{QueueReportBanner}", new string('*', 85));
                _logger.LogInformation("Job {JobId} — Reading queue report...", jobId);
                var queueSummary = await ReadQueueReportAsync(jobId);

                if (queueSummary.Status is "Completed" or "CompletedWithErrors" or "Failed")
                {
                    _logger.LogInformation("Migration job {JobId} finalized with queueStatus={Status}.", jobId, queueSummary.Status);
                        if (queueSummary.Status == "Failed")
                        {
                            var reason = queueSummary.FatalReason
                                ?? queueSummary.Errors.FirstOrDefault()
                                ?? "No specific reason provided by SPMI. See errors below.";
                            _logger.LogError("Failed reason: {Reason}", reason);
                        }
                        if (queueSummary.Errors.Count > 0)
                        {
                            _logger.LogInformation("---- Job errors (non-existence) ----");
                            foreach (var g in queueSummary.Errors
                                         .GroupBy(QueueErrorListGroupingKey, StringComparer.OrdinalIgnoreCase)
                                         .OrderByDescending(g => g.Count()))
                            {
                                var representative = g.First();
                                if (g.Count() == 1)
                                    _logger.LogError("{Error}", representative);
                                else
                                    _logger.LogError(
                                        "{Error} (repeated ×{RepeatCount} in job error list — same summary text; per-URL detail is in SPMI JobError lines above when logged)",
                                        representative,
                                        g.Count());
                            }

                            _logger.LogInformation("---- End errors ----");
                        }
                    return new MigrationJobInfo
                    {
                        JobId = jobId,
                        Status = queueSummary.Status,
                        Progress = 100,
                        CreatedDateTime = DateTime.UtcNow.ToString("O"),
                        ProcessedFileCount = queueSummary.FilesCreated,
                        TotalFileCount = expectedFileCount,
                        FailedFileCount = Math.Max(queueSummary.OtherErrorCount, 0),
                        AlreadyExistsCount = queueSummary.AlreadyExistsCount,
                        OtherErrorCount = queueSummary.OtherErrorCount,
                        Errors = queueSummary.Errors
                    };
                }

                if (wasActive)
                    _logger.LogInformation("Job {JobId} was active but final queue event not found yet; continuing to poll.", jobId);
                else
                    _logger.LogInformation("Job {JobId} not yet finalised (State=None, no final queue event); continuing to poll.", jobId);
            }

            await Task.Delay(pollIntervalMs);
        }

        throw new TimeoutException(
            $"Migration job {jobId} did not complete within {timeout.Value.TotalMinutes} minutes");
    }

    /// <summary>
    /// No-op: SharePoint-provisioned containers are managed by SharePoint and auto-expire.
    /// </summary>
    public Task CleanupStagingContainersAsync()
    {
        _logger.LogInformation("SharePoint-provisioned containers auto-expire; no cleanup needed.");
        return Task.CompletedTask;
    }

    // Queue reporting

    /// <summary>
    /// Reads SPMI queue messages until empty or a final event for this job is seen. Large jobs emit
    /// many JobProgress rows; a single 32-message receive without delete can hide JobEnd.
    /// </summary>
    private async Task<QueueSummary> ReadQueueReportAsync(Guid jobId)
    {
        var summary = new QueueSummary();

        if (string.IsNullOrEmpty(_queueUri))
            return summary;

        var queueClient = new QueueClient(new Uri(_queueUri));
        var receiveVisibility = TimeSpan.FromMinutes(5);
        const int maxRounds = 500;

        _logger.LogInformation("{QueueBanner}", new string('=', 85));
        _logger.LogInformation("SPMI migration queue — reading report messages for job {JobId}", jobId);

        // Same SharePoint failure (e.g. Save Conflict) often repeats per file — log full JSON once per signature per read.
        var jobDiagnosticDedup = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        for (var round = 0; round < maxRounds && !summary.SawFinalEvent; round++)
        {
            Azure.Response<QueueMessage[]> response;
            try
            {
                response = await queueClient.ReceiveMessagesAsync(maxMessages: 32, visibilityTimeout: receiveVisibility)
                    .ConfigureAwait(false);
            }
            catch (Exception ex) when (IsTransientRequestError(ex))
            {
                _logger.LogWarning(ex, "Transient queue-read error while checking job {JobId}.", jobId);
                return summary;
            }

            if (response.Value == null || response.Value.Length == 0)
                break;

            foreach (var msg in response.Value)
            {
                try
                {
                    var outerBody = msg.Body.ToString();
                    string decrypted;
                    try
                    {
                        decrypted = DecryptQueueMessage(outerBody);
                    }
                    catch
                    {
                        decrypted = outerBody;
                    }
                    var json = JObject.Parse(decrypted);
                    var eventType = json["Event"]?.ToString() ?? "";
                    var message = json["Message"]?.ToString()
                               ?? json["ErrorMessage"]?.ToString() ?? "";
                    var jobIdInMessage = json["JobId"]?.ToString();

                    if (!string.IsNullOrWhiteSpace(jobIdInMessage) &&
                        !string.Equals(jobIdInMessage, jobId.ToString(), StringComparison.OrdinalIgnoreCase))
                    {
                        await TryReleaseQueueMessageAsync(queueClient, msg).ConfigureAwait(false);
                        continue;
                    }

                    if (eventType.Contains("JobWarning", StringComparison.OrdinalIgnoreCase))
                    {
                        var preview = message;
                        var stackIdx = preview.IndexOf("CallStack --", StringComparison.OrdinalIgnoreCase);
                        if (stackIdx > 0)
                            preview = preview[..stackIdx].TrimEnd();
                        if (string.IsNullOrWhiteSpace(preview))
                            preview = "(no message body)";
                        const int maxPreviewChars = 450;
                        if (preview.Length > maxPreviewChars)
                            preview = preview[..maxPreviewChars] + " …";
                        _logger.LogWarning("SPMI JobWarning (JobId={JobId}): {Preview}", jobId, preview);
                    }

                    // Full payload at Debug. Omit raw JSON for high-noise or fully-diagnosed-at-Error events.
                    var omitQueueRawDebug =
                        eventType.Contains("JobProgress", StringComparison.OrdinalIgnoreCase)
                        || string.Equals(eventType, "JobError", StringComparison.OrdinalIgnoreCase)
                        || string.Equals(eventType, "JobFatalError", StringComparison.OrdinalIgnoreCase);
                    if (!omitQueueRawDebug)
                        _logger.LogDebug("Queue report raw: {Message}", decrypted);

                    if (eventType.Contains("JobEnd", StringComparison.OrdinalIgnoreCase) ||
                        eventType.Contains("JobError", StringComparison.OrdinalIgnoreCase) ||
                        decrypted.Contains("JobEnd", StringComparison.OrdinalIgnoreCase) ||
                        decrypted.Contains("JobError", StringComparison.OrdinalIgnoreCase))
                    {
                        summary.SawFinalEvent = true;
                    }

                    if (eventType.Contains("JobFatalError", StringComparison.OrdinalIgnoreCase))
                    {
                        // SharePoint sometimes raises JobFatalError for destination conflicts (file/folder
                        // already present). Treat those like Application 1 / DynamicETL-style re-runs: not a
                        // hard failure — the pipeline continues and post-job metadata patch can still run.
                        var isBenignFatal =
                            message.Contains("source type is not specified", StringComparison.OrdinalIgnoreCase) ||
                            message.Contains("SourceType", StringComparison.OrdinalIgnoreCase) ||
                            IsBenignMigrationConflictMessage(message);

                        if (isBenignFatal)
                        {
                            _logger.LogInformation("SPMI notice (non-fatal): [{Event}] {Message}", eventType, message);
                        }
                        else
                        {
                            summary.SawFatalError = true;
                            summary.FatalReason ??= string.IsNullOrWhiteSpace(message) ? "JobFatalError reported by SharePoint Migration API." : message;
                        }
                    }

                    if (eventType.Contains("Error", StringComparison.OrdinalIgnoreCase)
                        || eventType.Contains("Fail", StringComparison.OrdinalIgnoreCase)
                        || eventType.Contains("Warning", StringComparison.OrdinalIgnoreCase))
                    {
                        var normalizedMessage = message;
                        var callstackIndex = normalizedMessage.IndexOf("CallStack --", StringComparison.OrdinalIgnoreCase);
                        if (callstackIndex > 0)
                            normalizedMessage = normalizedMessage[..callstackIndex].TrimEnd();

                        if (IsBenignMigrationConflictMessage(normalizedMessage))
                        {
                            summary.AlreadyExistsCount++;
                            // Benign JobFatalError: count only — already logged in the JobFatalError block above.
                            if (!string.Equals(eventType, "JobFatalError", StringComparison.OrdinalIgnoreCase))
                            {
                                if (string.Equals(eventType, "JobError", StringComparison.OrdinalIgnoreCase))
                                {
                                    // Client: "already exists" on re-runs — WRN, not ERR; not an app-thrown exception.
                                    if (_settings.ReportExistingFilesAsOverwritten)
                                        _logger.LogWarning(
                                            "SPMI JobError: destination already exists (overwrite intent). Queue text may mention COM; application does not throw: {Message}",
                                            normalizedMessage);
                                    else
                                        _logger.LogWarning(
                                            "SPMI JobError: destination already exists (skipped). Queue text may mention COM; application does not throw: {Message}",
                                            normalizedMessage);
                                }
                                else if (_settings.ReportExistingFilesAsOverwritten)
                                    _logger.LogInformation("Destination conflict treated as OK (overwrite/skip): {Message}", normalizedMessage);
                                else
                                    _logger.LogInformation("Skipped (exists): {Message}", normalizedMessage);
                            }
                        }
                        else if (normalizedMessage.Contains("The source type is not specified", StringComparison.OrdinalIgnoreCase))
                        {
                            _logger.LogInformation("SPMI notice: {Message}", normalizedMessage);
                        }
                        else
                        {
                            if (eventType.Contains("Error", StringComparison.OrdinalIgnoreCase))
                                summary.OtherErrorCount++;

                            summary.Errors.Add($"[{eventType}] {normalizedMessage}");
                            var isWarning = eventType.Contains("Warning", StringComparison.OrdinalIgnoreCase);
                            // JobWarning already logged above as WRN with a clear prefix; avoid duplicate lines.
                            if (isWarning && !string.Equals(eventType, "JobWarning", StringComparison.OrdinalIgnoreCase))
                                _logger.LogWarning("Migration issue: [{Event}] {Message}", eventType, normalizedMessage);
                            else if (!isWarning)
                            {
                                var isJobLevelDiagnostic = eventType.Contains("JobError", StringComparison.OrdinalIgnoreCase)
                                    || eventType.Contains("JobFatalError", StringComparison.OrdinalIgnoreCase);
                                if (isJobLevelDiagnostic)
                                    LogSpmiJobQueueDiagnostics(jobId, eventType, json, normalizedMessage, decrypted, jobDiagnosticDedup);
                                else
                                    _logger.LogError("Migration issue: [{Event}] {Message}", eventType, normalizedMessage);
                            }
                        }
                    }

                    if (eventType.Contains("JobProgress", StringComparison.OrdinalIgnoreCase))
                    {
                        summary.FilesCreated = Math.Max(summary.FilesCreated, ParseInt(json["FilesCreated"]?.ToString()));
                        summary.TotalErrors = Math.Max(summary.TotalErrors, ParseInt(json["TotalErrors"]?.ToString()));
                        var totalWarnings = ParseInt(json["TotalWarnings"]?.ToString());
                        var objectsProcessed = ParseInt(json["ObjectsProcessed"]?.ToString());

                        var nowUtc = DateTime.UtcNow;
                        var changed = summary.FilesCreated != _lastQueueFilesCreated || summary.TotalErrors != _lastQueueErrors;
                        var intervalElapsed = (nowUtc - _lastQueueProgressLogUtc) >= TimeSpan.FromSeconds(30);
                        if (changed || intervalElapsed)
                        {
                            _lastQueueFilesCreated = summary.FilesCreated;
                            _lastQueueErrors = summary.TotalErrors;
                            _lastQueueProgressLogUtc = nowUtc;
                            _logger.LogInformation(
                                "Queue progress: created={FilesCreated}, objects={ObjectsProcessed}, errors={Errors}, warnings={Warnings}",
                                summary.FilesCreated, objectsProcessed, summary.TotalErrors, totalWarnings);
                        }
                    }

                    if (eventType.Contains("JobEnd", StringComparison.OrdinalIgnoreCase))
                    {
                        summary.FilesCreated = ParseInt(json["FilesCreated"]?.ToString());
                        summary.TotalErrors = ParseInt(json["TotalErrors"]?.ToString());
                    }

                    await TryDeleteQueueMessageAsync(queueClient, msg).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Could not parse or handle SPMI queue message for job {JobId}.", jobId);
                    await TryReleaseQueueMessageAsync(queueClient, msg).ConfigureAwait(false);
                }
            }
        }

        if (summary.SawFinalEvent)
        {
            if (summary.SawFatalError)
            {
                summary.Status = "Failed";
            }
            else
            {
                summary.Status = summary.OtherErrorCount > 0 ? "CompletedWithErrors" : "Completed";
            }
        }

        LogRepeatedSpmiQueueErrorSummary(jobId, summary.Errors);
        return summary;
    }

    /// <summary>Groups <c>[JobError] message…</c> lines that share the same first line of body text.</summary>
    private static string QueueErrorListGroupingKey(string entry)
    {
        var t = (entry ?? string.Empty).Trim();
        var bracket = t.IndexOf(']');
        var body = bracket >= 0 && bracket + 1 < t.Length ? t[(bracket + 1)..].Trim() : t;
        var firstNl = body.IndexOf('\n');
        if (firstNl > 0)
            body = body[..firstNl].Trim();
        return string.IsNullOrEmpty(body) ? t : body;
    }

    /// <summary>
    /// When many queue errors share the same opening line (e.g. every file hits the same Save Conflict),
    /// emit one clear summary so logs are easier to interpret for large jobs (5k–10k files).
    /// </summary>
    private void LogRepeatedSpmiQueueErrorSummary(Guid jobId, IReadOnlyList<string> errors)
    {
        if (errors.Count < 2)
            return;

        var groups = errors
            .GroupBy(QueueErrorListGroupingKey, StringComparer.OrdinalIgnoreCase)
            .OrderByDescending(g => g.Count())
            .ToList();
        var dominant = groups[0];
        if (dominant.Count() < 2)
            return;

        var ratio = (double)dominant.Count() / errors.Count;
        if (dominant.Count() < 3 && ratio < 0.75)
            return;

        var snippet = dominant.Key.Length > 200 ? dominant.Key[..200] + "…" : dominant.Key;
        _logger.LogWarning(
            "SPMI job {JobId}: {Total} queue error line(s); {RepeatCount} share the same first-line text ({Ratio:P0} of lines): \"{Snippet}\". " +
            "Repeated identical wording across many files usually means one systemic SharePoint import condition (not a separate bug per file).",
            jobId,
            errors.Count,
            dominant.Count(),
            ratio,
            snippet);
    }

    /// <summary>
    /// Logs structured fields from SPMI queue JSON so JobError / JobFatalError can be diagnosed from logs
    /// without guessing (Url, ErrorCode, ObjectType, etc.). Raw JSON is logged at Error (size-capped) so
    /// log filters show the full diagnostic under [ERR] with the structured line above.
    /// When <paramref name="jobDiagnosticDedup"/> is provided, repeated identical ErrorCode + first message line
    /// in the same queue read emit a compact [ERR] only (avoids hundreds of duplicate multi-KB JSON blocks).
    /// </summary>
    private void LogSpmiJobQueueDiagnostics(
        Guid pipelineJobId,
        string eventType,
        JObject json,
        string messageSummary,
        string fullDecryptedPayload,
        Dictionary<string, int>? jobDiagnosticDedup = null)
    {
        var queueJobId = json["JobId"]?.ToString() ?? string.Empty;
        var objectType = json["ObjectType"]?.ToString() ?? string.Empty;
        var url = json["Url"]?.ToString() ?? string.Empty;
        var errorCode = json["ErrorCode"]?.ToString() ?? string.Empty;
        var errorType = json["ErrorType"]?.ToString() ?? string.Empty;
        var itemId = json["Id"]?.ToString() ?? string.Empty;
        var direction = json["MigrationDirection"]?.ToString() ?? string.Empty;
        var migrationType = json["MigrationType"]?.ToString() ?? string.Empty;
        var time = json["Time"]?.ToString() ?? string.Empty;

        var msg = string.IsNullOrWhiteSpace(messageSummary) ? "(no message body)" : messageSummary;

        if (jobDiagnosticDedup != null &&
            (string.Equals(eventType, "JobError", StringComparison.OrdinalIgnoreCase) ||
             string.Equals(eventType, "JobFatalError", StringComparison.OrdinalIgnoreCase)))
        {
            var firstLine = msg;
            var nl = firstLine.IndexOf('\n');
            if (nl > 0)
                firstLine = firstLine[..nl].Trim();
            if (firstLine.Length > 400)
                firstLine = firstLine[..400] + "…";
            var signature = $"{eventType}|{errorCode}|{firstLine}";
            if (jobDiagnosticDedup.TryGetValue(signature, out var prior))
            {
                var occurrence = prior + 1;
                jobDiagnosticDedup[signature] = occurrence;
                _logger.LogError(
                    "SPMI {Event} (repeated pattern #{Occurrence} in this queue read; full structured + JSON omitted — same ErrorCode and first line as earlier): PipelineJobId={PipelineJobId} Url={Url} ItemId={ItemId} ErrorCode={ErrorCode} MessageFirstLine={FirstLine}",
                    eventType,
                    occurrence,
                    pipelineJobId,
                    url,
                    itemId,
                    errorCode,
                    firstLine);
                return;
            }

            jobDiagnosticDedup[signature] = 1;
        }

        _logger.LogError(
            "SPMI {Event}: PipelineJobId={PipelineJobId} QueueJobId={QueueJobId} Time={Time} ObjectType={ObjectType} Url={Url} ErrorCode={ErrorCode} ErrorType={ErrorType} ItemId={ItemId} MigrationDirection={Direction} MigrationType={MigrationType} Message={Message}",
            eventType,
            pipelineJobId,
            queueJobId,
            time,
            objectType,
            url,
            errorCode,
            errorType,
            itemId,
            direction,
            migrationType,
            msg);

        const int maxRawChars = 16000;
        if (string.IsNullOrEmpty(fullDecryptedPayload))
            return;
        var raw = fullDecryptedPayload.Length <= maxRawChars
            ? fullDecryptedPayload
            : fullDecryptedPayload[..maxRawChars] + " …(truncated)";
        _logger.LogError("SPMI queue JSON (JobError diagnostic, PipelineJobId={PipelineJobId}): {RawJson}", pipelineJobId, raw);
    }

    private static async Task TryDeleteQueueMessageAsync(QueueClient client, QueueMessage msg)
    {
        try
        {
            await client.DeleteMessageAsync(msg.MessageId, msg.PopReceipt).ConfigureAwait(false);
        }
        catch
        {
            // Pop receipt may have expired.
        }
    }

    private static async Task TryReleaseQueueMessageAsync(QueueClient client, QueueMessage msg)
    {
        try
        {
            await client.UpdateMessageAsync(msg.MessageId, msg.PopReceipt, visibilityTimeout: TimeSpan.Zero)
                .ConfigureAwait(false);
        }
        catch
        {
            // Pop receipt may have expired.
        }
    }

    private sealed class QueueSummary
    {
        public string Status { get; set; } = "InProgress";
        public List<string> Errors { get; } = new();
        public bool SawFinalEvent { get; set; }
        public bool SawFatalError { get; set; }
        public string? FatalReason { get; set; }
        public int FilesCreated { get; set; }
        public int TotalErrors { get; set; }
        public int AlreadyExistsCount { get; set; }
        public int OtherErrorCount { get; set; }
    }

    private static int ParseInt(string? value)
    {
        return int.TryParse(value, out var parsed) ? parsed : 0;
    }

    /// <summary>
    /// SPMI queue messages that indicate the destination already has the object — not a blocking error
    /// for re-runs (client: same as tolerating overwrite / existing content).
    /// </summary>
    private static bool IsBenignMigrationConflictMessage(string message)
    {
        if (string.IsNullOrWhiteSpace(message))
            return false;

        return message.Contains("already exists", StringComparison.OrdinalIgnoreCase)
               || message.Contains("already been added", StringComparison.OrdinalIgnoreCase)
               || message.Contains("item already exists", StringComparison.OrdinalIgnoreCase)
               || message.Contains("folder already exists", StringComparison.OrdinalIgnoreCase)
               || message.Contains("file already exists", StringComparison.OrdinalIgnoreCase)
               || message.Contains("duplicate name", StringComparison.OrdinalIgnoreCase)
               || message.Contains("same name already exists", StringComparison.OrdinalIgnoreCase)
               || message.Contains("a file with the same name", StringComparison.OrdinalIgnoreCase)
               || message.Contains("name already exists", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsTransientRequestError(Exception ex)
    {
        if (ex is HttpRequestException || ex is TaskCanceledException)
            return true;
        if (ex is WebException wex &&
            (wex.Status == WebExceptionStatus.Timeout ||
             wex.Status == WebExceptionStatus.ConnectFailure ||
             wex.Status == WebExceptionStatus.NameResolutionFailure ||
             wex.Status == WebExceptionStatus.ConnectionClosed))
            return true;

        if (ex is Microsoft.SharePoint.Client.ClientRequestException crex &&
            crex.Message.Contains("while sending the request", StringComparison.OrdinalIgnoreCase))
            return true;

        return ex.InnerException is HttpRequestException;
    }

    private async Task ExecuteQueryWithRetryAsync(int maxAttempts = 4)
    {
        for (int attempt = 1; attempt <= maxAttempts; attempt++)
        {
            try
            {
                await _clientContextG.ExecuteQueryAsync();
                return;
            }
            catch (Exception ex) when (IsTransientRequestError(ex) && attempt < maxAttempts)
            {
                var delay = TimeSpan.FromSeconds(Math.Min(20, Math.Pow(2, attempt)));
                _logger.LogWarning(ex,
                    "Transient CSOM timeout/request error (attempt {Attempt}/{MaxAttempts}); retrying in {Delay}s...",
                    attempt, maxAttempts, delay.TotalSeconds);
                await Task.Delay(delay);
            }
        }

        // final attempt propagates original exception for caller handling
        await _clientContextG.ExecuteQueryAsync();
    }

    /// <summary>
    /// Decrypts an encrypted queue message using the AES-256-CBC key from provisioned containers.
    /// Message format: base64 → JSON { Label, JobId, IV, Content } → AES decrypt Content with IV.
    /// </summary>
    private string DecryptQueueMessage(string base64Message)
    {
        var json = Newtonsoft.Json.Linq.JObject.Parse(
            System.Text.Encoding.UTF8.GetString(Convert.FromBase64String(base64Message)));

        var iv = Convert.FromBase64String(json["IV"]!.ToString());
        var content = Convert.FromBase64String(json["Content"]!.ToString());

        using var aes = System.Security.Cryptography.Aes.Create();
        aes.Key = _encryptionKey;
        aes.IV = iv;
        aes.Mode = System.Security.Cryptography.CipherMode.CBC;
        aes.Padding = System.Security.Cryptography.PaddingMode.PKCS7;

        using var decryptor = aes.CreateDecryptor();
        var decryptedBytes = decryptor.TransformFinalBlock(content, 0, content.Length);
        return System.Text.Encoding.UTF8.GetString(decryptedBytes);
    }

    // Manifest generation

    /// <summary>
    /// Generates all 8 required XML manifest files for the SPMI package.
    /// </summary>
    private Dictionary<string, string> GenerateManifestPackage(
        List<FileRecord> records, Guid jobId, string webUrl)
    {
        var manifests = new Dictionary<string, string>
        {
            ["ExportSettings.xml"] = GenerateExportSettings(),
            ["LookupListMap.xml"] = GenerateLookupListMap(),
            ["Manifest.xml"] = GenerateManifest(records, webUrl),
            ["Requirements.xml"] = GenerateRequirements(),
            ["RootObjectMap.xml"] = GenerateRootObjectMap(webUrl),
            ["SystemData.xml"] = GenerateSystemData(webUrl),
            ["UserGroup.xml"] = GenerateUserGroupMap(),
            ["ViewFormsList.xml"] = GenerateViewFormsList()
        };

        return manifests;
    }

    private string GenerateExportSettings()
    {
        XNamespace ns = "urn:deployment-exportsettings-schema";
        return XmlToString(new XDocument(
            new XDeclaration("1.0", "utf-8", null),
            new XElement(ns + "ExportSettings",
                new XAttribute("SiteUrl", _settings.SharePointSiteUrl),
                new XAttribute("FileLocation", string.Empty),
                new XAttribute("IncludeSecurity", "All"),
                new XAttribute("IncludeVersions", "LastMajor"),
                new XAttribute("ExportMethod", "ExportAll")
                )));
    }

    private string GenerateLookupListMap()
    {
        // No namespace — SharePoint's schema validation rejects the lookuplistmap namespace
        return XmlToString(new XDocument(
            new XDeclaration("1.0", "utf-8", null),
            new XElement("LookupListMap")));
    }

    private string GenerateManifest(List<FileRecord> records, string webUrl)
    {
        XNamespace ns = "urn:deployment-manifest-schema";
        var root = new XElement(ns + "SPObjects");

        // Track emitted folders
        var emittedFolders = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var record in records)
        {
            // Ensure manifest paths are SharePoint-safe (special chars, trailing dots/spaces, etc.)
            // This also prevents folder-create fatal errors like: Cannot create folder "... (ref: 123) ..."
            var targetPath = PathTransformService.SanitizeSharePointRelativePath(
                record.MappedPath.Replace('\\', '/').TrimStart('/'));
            var parts = targetPath.Split('/');

            // 3. Folder objects for all intermediate directories
            var folderAccumulator = string.Empty;
            for (int i = 0; i < parts.Length - 1; i++)
            {
                folderAccumulator = string.IsNullOrEmpty(folderAccumulator)
                    ? parts[i]
                    : $"{folderAccumulator}/{parts[i]}";

                if (emittedFolders.Add(folderAccumulator))
                {
                    var folderId = GenerateDeterministicGuid(folderAccumulator);
                    var parentPath = i == 0 ? string.Empty : folderAccumulator[..folderAccumulator.LastIndexOf('/')];
                    var parentFolderId = string.IsNullOrEmpty(parentPath)
                        ? _rootFolderId
                        : GenerateDeterministicGuid(parentPath).ToString();

                    var folderElement = new XElement(ns + "Folder",
                        new XAttribute("Id", folderId),
                        new XAttribute("Url", $"{_rootFolderUrl}/{folderAccumulator}"),
                        new XAttribute("ParentFolderId", parentFolderId),
                        new XAttribute("ParentWebId", _webId),
                        new XAttribute("ParentWebUrl", webUrl),
                        new XAttribute("Name", parts[i]));
                    // Properties are NOT embedded in the SPMI manifest — SPMI schema rejects them
                    // and fails the entire job. CaseId/CaseType are patched via bulk CSOM post-upload.

                    root.Add(new XElement(ns + "SPObject",
                        new XAttribute("Id", folderId),
                        new XAttribute("ObjectType", "SPFolder"),
                        new XAttribute("ParentId", parentFolderId),
                        new XAttribute("ParentWebId", _webId),
                        new XAttribute("ParentWebUrl", webUrl),
                        folderElement));
                }
            }

            // 4. File object
            var fileId = GenerateDeterministicGuid($"file:{targetPath}");
            var fileName = parts[^1];
            var fileParentPath = parts.Length > 1 ? string.Join("/", parts[..^1]) : string.Empty;
            var fileParentId = string.IsNullOrEmpty(fileParentPath)
                ? _rootFolderId
                : GenerateDeterministicGuid(fileParentPath).ToString();

            // FileValue = blob name in the data container (must match what we uploaded)
            var fileElement = new XElement(ns + "SPObject",
                new XAttribute("Id", fileId),
                new XAttribute("ObjectType", "SPFile"),
                new XAttribute("ParentId", fileParentId),
                new XAttribute("ParentWebId", _webId),
                new XAttribute("ParentWebUrl", webUrl),
                new XElement(ns + "File",
                    new XAttribute("Url", $"{_rootFolderUrl}/{targetPath}"),
                    new XAttribute("Id", fileId),
                    new XAttribute("ParentWebId", _webId),
                    new XAttribute("ParentWebUrl", webUrl),
                    new XAttribute("Name", fileName),
                    new XAttribute("ListItemIntId", "0"),
                    new XAttribute("ListId", _listId),
                    new XAttribute("ParentId", fileParentId),
                    new XAttribute("TimeCreated", ToIso8601(record.CreatedOn)),
                    new XAttribute("TimeLastModified", ToIso8601(record.LastModified)),
                    new XAttribute("Version", "1.0"),
                    new XAttribute("FileValue", targetPath),
                    new XAttribute("Author", "1"),
                    new XAttribute("ModifiedBy", "1")));

            // Properties are NOT embedded in the SPMI manifest — SPMI schema rejects them and fails
            // the entire job before a single file is processed. CaseId/CaseType are applied via
            // bulk CSOM post-upload (PatchCaseMetadataBulkAsync).

            root.Add(fileElement);
        }

        return XmlToString(new XDocument(new XDeclaration("1.0", "utf-8", null), root));
    }

    private string GenerateRequirements()
    {
        XNamespace ns = "urn:deployment-requirements-schema";
        return XmlToString(new XDocument(
            new XDeclaration("1.0", "utf-8", null),
            new XElement(ns + "Requirements")));
    }

    private string GenerateRootObjectMap(string webUrl)
    {
        XNamespace ns = "urn:deployment-rootobjectmap-schema";
        return XmlToString(new XDocument(
            new XDeclaration("1.0", "utf-8", null),
            new XElement(ns + "RootObjects",
                new XElement(ns + "RootObject",
                    new XAttribute("Id", _webId),
                    new XAttribute("Type", "Web"),
                    new XAttribute("ParentId", _siteId),
                    new XAttribute("WebUrl", webUrl),
                    new XAttribute("Url", webUrl),
                    new XAttribute("IsDirty", "true")),
                new XElement(ns + "RootObject",
                    new XAttribute("Id", _listId),
                    new XAttribute("Type", "List"),
                    new XAttribute("ParentId", _webId),
                    new XAttribute("WebUrl", webUrl),
                    new XAttribute("Url", _rootFolderUrl),
                    new XAttribute("IsDirty", "true")))));
    }

    private string GenerateSystemData(string webUrl)
    {
        XNamespace ns = "urn:deployment-systemdata-schema";
        return XmlToString(new XDocument(
            new XDeclaration("1.0", "utf-8", null),
            new XElement(ns + "SystemData",
                new XElement(ns + "SchemaVersion",
                    new XAttribute("Version", "15.0.0.0"),
                    new XAttribute("Build", "16.0.0.0"),
                    new XAttribute("DatabaseVersion", "11552"),
                    new XAttribute("SiteVersion", "15")),
                new XElement(ns + "ManifestFiles",
                    new XElement(ns + "ManifestFile",
                        new XAttribute("Name", "Manifest.xml"))),
                new XElement(ns + "SystemObjects",
                    new XElement(ns + "SystemObject",
                        new XAttribute("Id", _siteId),
                        new XAttribute("Type", "Site"),
                        new XAttribute("Url", _settings.SharePointSiteUrl)),
                    new XElement(ns + "SystemObject",
                        new XAttribute("Id", _webId),
                        new XAttribute("Type", "Web"),
                        new XAttribute("Url", webUrl))),
                new XElement(ns + "RootWebOnlyLists"))));
    }

    private string GenerateUserGroupMap()
    {
        XNamespace ns = "urn:deployment-usergroupmap-schema";
        return XmlToString(new XDocument(
            new XDeclaration("1.0", "utf-8", null),
            new XElement(ns + "UserGroupMap",
                new XElement(ns + "Users",
                    new XElement(ns + "User",
                        new XAttribute("Id", "1"),
                        new XAttribute("Name", "System Account"),
                        new XAttribute("Login", "SHAREPOINT\\system"),
                        new XAttribute("SystemId", Convert.ToBase64String(
                            System.Text.Encoding.UTF8.GetBytes("SHAREPOINT\\system"))),
                        new XAttribute("IsSiteAdmin", "true"),
                        new XAttribute("IsDomainGroup", "false"),
                        new XAttribute("IsDeleted", "false"),
                        new XAttribute("Flags", "0"))),
                new XElement(ns + "Groups"))));
    }

    private string GenerateViewFormsList()
    {
        XNamespace ns = "urn:deployment-viewformlist-schema";
        return XmlToString(new XDocument(
            new XDeclaration("1.0", "utf-8", null),
            new XElement(ns + "ViewFormsList")));
    }

    private static string XmlToString(XDocument doc)
    {
        // Use a MemoryStream with UTF-8 encoding to avoid the StringWriter UTF-16 issue.
        // SharePoint's XML parser expects UTF-8 with no BOM mismatch.
        using var ms = new MemoryStream();
        using (var xw = System.Xml.XmlWriter.Create(ms, new System.Xml.XmlWriterSettings
        {
            Encoding = new System.Text.UTF8Encoding(false), // no BOM
            Indent = true,
            OmitXmlDeclaration = false
        }))
        {
            doc.WriteTo(xw);
        }
        return System.Text.Encoding.UTF8.GetString(ms.ToArray());
    }

    // Helpers

    /// <summary>
    /// Generates a deterministic GUID from a string path so that folder/file IDs
    /// are consistent across manifest generation runs.
    /// </summary>
    /// <summary>
    /// Encrypts a stream with AES-256-CBC and uploads it, storing the IV in blob metadata.
    /// SharePoint's migration engine reads the IV from metadata key "IV" to decrypt.
    /// </summary>
    private async Task UploadEncryptedBlobAsync(BlobContainerClient container, string blobName, Stream plainStream)
    {
        using var aes = System.Security.Cryptography.Aes.Create();
        aes.Key = _encryptionKey;
        aes.GenerateIV();
        aes.Mode = System.Security.Cryptography.CipherMode.CBC;
        aes.Padding = System.Security.Cryptography.PaddingMode.PKCS7;

        using var encryptedStream = new MemoryStream();
        using (var cryptoStream = new System.Security.Cryptography.CryptoStream(
            encryptedStream, aes.CreateEncryptor(), System.Security.Cryptography.CryptoStreamMode.Write, leaveOpen: true))
        {
            await plainStream.CopyToAsync(cryptoStream);
        }
        encryptedStream.Position = 0;

        var blobClient = container.GetBlobClient(blobName);
        await blobClient.UploadAsync(encryptedStream, new BlobUploadOptions
        {
            Metadata = new Dictionary<string, string>
            {
                ["IV"] = Convert.ToBase64String(aes.IV)
            }
        });
    }

    /// <summary>
    /// Converts a date string like "2026-03-23 08:40:35" to ISO 8601 "2026-03-23T08:40:35Z".
    /// </summary>
    private static string ToIso8601(string dateStr)
    {
        if (DateTime.TryParse(dateStr, out var dt))
            return dt.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ");
        return DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ");
    }

    private static Guid GenerateDeterministicGuid(string input)
    {
        using var md5 = System.Security.Cryptography.MD5.Create();
        var hash = md5.ComputeHash(System.Text.Encoding.UTF8.GetBytes(input));
        return new Guid(hash);
    }

    private async Task EnsureMetadataFieldMappingsAsync(IEnumerable<FileRecord> records, string libraryTitle)
    {
        // Metadata field resolution is best-effort. Any failure must NOT block file uploads.
        try
        {
            _effectiveMetadataFieldMap.Clear();
            foreach (var kvp in _settings.MetadataFieldMap)
            {
                if (!string.IsNullOrWhiteSpace(kvp.Key) && !string.IsNullOrWhiteSpace(kvp.Value))
                    _effectiveMetadataFieldMap[kvp.Key] = kvp.Value;
            }

            var requiredKeys = records
                .Where(r => r.Metadata is { Count: > 0 })
                .SelectMany(r => r.Metadata.Keys)
                .Where(k => !string.IsNullOrWhiteSpace(k))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();

            // Create/resolve columns for configured SimpleETL display names even when no file had that
            // metadata key this run (e.g. DocumentId not in XML — otherwise "Document ID" column was never ensured).
            var keysToResolve = new HashSet<string>(requiredKeys, StringComparer.OrdinalIgnoreCase);
            foreach (var candidate in new[] { "CaseId", "CaseType", "DocumentId" })
            {
                if (!string.IsNullOrWhiteSpace(GetMetadataDisplayName(candidate)))
                    keysToResolve.Add(candidate);
            }

            if (keysToResolve.Count == 0)
                return;

            var extrasConfigured = keysToResolve
                .Where(k => !requiredKeys.Any(r => string.Equals(r, k, StringComparison.OrdinalIgnoreCase)))
                .ToList();
            if (extrasConfigured.Count > 0)
            {
                _logger.LogInformation(
                    "Ensuring SharePoint list columns for metadata keys configured in SimpleETL but absent on all records this run: {Keys} (library '{Library}').",
                    string.Join(", ", extrasConfigured),
                    libraryTitle);
            }

            if (!_resolvedMetadataFieldMapByLibrary.TryGetValue(libraryTitle, out var libraryCache))
            {
                libraryCache = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                _resolvedMetadataFieldMapByLibrary[libraryTitle] = libraryCache;
            }

            foreach (var metadataKey in keysToResolve)
            {
                if (_effectiveMetadataFieldMap.ContainsKey(metadataKey))
                    continue;

                var displayName = GetMetadataDisplayName(metadataKey);
                if (string.IsNullOrWhiteSpace(displayName))
                    continue;

                if (!libraryCache.TryGetValue(metadataKey, out var internalName))
                {
                    if (string.Equals(metadataKey, "CaseId", StringComparison.OrdinalIgnoreCase) ||
                        string.Equals(metadataKey, "CaseType", StringComparison.OrdinalIgnoreCase) ||
                        string.Equals(metadataKey, "DocumentId", StringComparison.OrdinalIgnoreCase))
                    {
                        await EnsureCaseMetadataTextFieldAsync(libraryTitle, displayName).ConfigureAwait(false);
                    }

                    internalName = await ResolveFieldInternalNameAsync(libraryTitle, displayName).ConfigureAwait(false) ?? string.Empty;
                    libraryCache[metadataKey] = internalName;
                }

                if (!string.IsNullOrWhiteSpace(internalName))
                {
                    _effectiveMetadataFieldMap[metadataKey] = internalName;
                    _logger.LogInformation("Resolved SharePoint metadata field mapping: {MetadataKey} -> {InternalName} (library: {Library})",
                        metadataKey, internalName, libraryTitle);
                }
                else
                {
                    _logger.LogWarning("Could not resolve SharePoint field internal name for '{MetadataKey}' (display: '{DisplayName}') in library '{Library}'. Metadata will be skipped for this field; files will still be copied.",
                        metadataKey, displayName, libraryTitle);
                }
            }

            if (keysToResolve.Count > 0)
            {
                var mapped = string.Join(", ", _effectiveMetadataFieldMap.Select(kvp => $"{kvp.Key}→{kvp.Value}"));
                _logger.LogInformation("Metadata field resolution for library '{Library}': {MappedCount} mapped keys [{Mapped}]",
                    libraryTitle, _effectiveMetadataFieldMap.Count, mapped);
            }
        }
        catch (Exception ex)
        {
            // Field resolution is non-critical. Log and proceed so files are always copied.
            _logger.LogWarning(ex,
                "Metadata field resolution failed for library '{Library}'. Files will still be copied without metadata column mapping.",
                libraryTitle);
            _effectiveMetadataFieldMap.Clear();
        }
    }

    private string? GetMetadataDisplayName(string metadataKey)
    {
        if (string.Equals(metadataKey, "CaseId", StringComparison.OrdinalIgnoreCase))
            return _processFlags.GetSection("SHAREPOINT_CASEID_COLUMN_DISPLAY_NAME").Value;
        if (string.Equals(metadataKey, "CaseType", StringComparison.OrdinalIgnoreCase))
            return _processFlags.GetSection("SHAREPOINT_CASETYPE_COLUMN_DISPLAY_NAME").Value;
        if (string.Equals(metadataKey, "DocumentId", StringComparison.OrdinalIgnoreCase))
            return _processFlags.GetSection("SHAREPOINT_DOCUMENTID_COLUMN_DISPLAY_NAME").Value;

        return null;
    }

    /// <summary>
    /// Creates a single-line text column on the document library if it does not already exist
    /// (CaseId / CaseType / DocumentId). Does not throw; logs on failure.
    /// </summary>
    private async Task EnsureCaseMetadataTextFieldAsync(string libraryTitle, string displayName)
    {
        if (string.IsNullOrWhiteSpace(displayName))
            return;

        var existing = await ResolveFieldInternalNameAsync(libraryTitle, displayName).ConfigureAwait(false);
        if (!string.IsNullOrWhiteSpace(existing))
            return;

        try
        {
            var list = _web.Lists.GetByTitle(libraryTitle);
            var escaped = SecurityElement.Escape(displayName) ?? displayName;
            var schemaXml = "<Field Type='Text' DisplayName='" + escaped + "' />";
            list.Fields.AddFieldAsXml(schemaXml, addToDefaultView: false, AddFieldOptions.DefaultValue);
            await ExecuteQueryWithRetryAsync().ConfigureAwait(false);
            _logger.LogInformation("Created text column '{DisplayName}' on library '{Library}'.", displayName, libraryTitle);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "Could not create text column '{DisplayName}' on library '{Library}'. Create the column manually or check permissions.",
                displayName, libraryTitle);
        }
    }

    private async Task<string?> ResolveFieldInternalNameAsync(string libraryTitle, string displayName)
    {
        try
        {
            var list = _web.Lists.GetByTitle(libraryTitle);
            var fields = list.Fields;
            _clientContextG.Load(fields, fs => fs.Include(f => f.InternalName, f => f.Title));
            await ExecuteQueryWithRetryAsync().ConfigureAwait(false);

            var field = fields.FirstOrDefault(f =>
                string.Equals(f.Title, displayName, StringComparison.OrdinalIgnoreCase) ||
                string.Equals(f.InternalName, displayName, StringComparison.OrdinalIgnoreCase));

            return field?.InternalName;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "Could not read fields for library '{Library}' to resolve display name '{DisplayName}'. Field mapping skipped.",
                libraryTitle, displayName);
            return null;
        }
    }

    // ── Bulk CSOM metadata patch ──────────────────────────────────────────────────────────────────

    /// <summary>
    /// After SPMI jobs complete, patches CaseId, CaseType, and DocumentId (when present on records)
    /// on SharePoint list items. Uses <c>GetFileByServerRelativeUrl</c> for each <see cref="FileRecord.MappedPath"/>
    /// (batched) so the library is not enumerated with CAML — that avoids the list view threshold on large
    /// document libraries. Only files present in the migration <paramref name="records"/> batch are updated.
    /// </summary>
    /// <param name="records">Records whose Metadata contains CaseId/CaseType/DocumentId values.</param>
    /// <param name="libraryTitle">SharePoint document library title (e.g. "2010").</param>
    /// <param name="yearPrefixToStrip">
    /// When YearAsLibrary is true, pass the year string (e.g. "2010") so it is stripped from
    /// MappedPath before deriving the folder path within the library.
    /// </param>
    public async Task<int> PatchCaseMetadataBulkAsync(
        IReadOnlyList<FileRecord> records,
        string libraryTitle,
        string? yearPrefixToStrip = null)
    {
        if (records.Count == 0) return 0;

        // Resolve CaseId / CaseType / DocumentId internal field names (best-effort; skip if unavailable).
        string? caseIdField = null;
        string? caseTypeField = null;
        string? documentIdField = null;
        try
        {
            await EnsureMetadataFieldMappingsAsync(records, libraryTitle).ConfigureAwait(false);
            _effectiveMetadataFieldMap.TryGetValue("CaseId", out caseIdField);
            _effectiveMetadataFieldMap.TryGetValue("CaseType", out caseTypeField);
            _effectiveMetadataFieldMap.TryGetValue("DocumentId", out documentIdField);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not resolve metadata field names for bulk patch on '{Library}'. Skipping.", libraryTitle);
            return 0;
        }

        if (string.IsNullOrWhiteSpace(caseIdField) && string.IsNullOrWhiteSpace(caseTypeField) &&
            string.IsNullOrWhiteSpace(documentIdField))
        {
            _logger.LogInformation("No CaseId/CaseType/DocumentId fields configured for library '{Library}'; skipping bulk metadata patch.", libraryTitle);
            return 0;
        }

        _logger.LogInformation(
            "Bulk metadata field mapping for '{Library}': CaseIdField={CaseIdField}, CaseTypeField={CaseTypeField}, DocumentIdField={DocumentIdField}",
            libraryTitle,
            caseIdField ?? "(none)",
            caseTypeField ?? "(none)",
            documentIdField ?? "(none)");

        // Load the library root folder URL for building FolderServerRelativeUrl in CAML queries.
        var list = _web.Lists.GetByTitle(libraryTitle);
        _clientContextG.Load(list, l => l.RootFolder.ServerRelativeUrl);
        await ExecuteQueryWithRetryAsync().ConfigureAwait(false);
        var rootFolderUrl = list.RootFolder.ServerRelativeUrl.TrimEnd('/');

        // Group records by their case folder path (directory part, relative to library root).
        // MappedPath example (YearAsLibrary): "2010/530341/filename.docx"
        // After stripping yearPrefix: "530341/filename.docx" → folder = "530341"
        var caseGroups = records
            .Where(r => r.Metadata.ContainsKey("CaseId") || r.Metadata.ContainsKey("CaseType") ||
                        r.Metadata.ContainsKey("DocumentId"))
            .GroupBy(r =>
            {
                var path = (r.MappedPath ?? string.Empty).Replace('\\', '/').TrimStart('/');
                if (!string.IsNullOrEmpty(yearPrefixToStrip))
                {
                    var prefix = yearPrefixToStrip.TrimEnd('/') + "/";
                    if (path.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                        path = path[prefix.Length..];
                }
                var segs = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
                // Return the directory containing the file (all segments except the last).
                return segs.Length >= 2 ? string.Join("/", segs[..^1]) : string.Empty;
            }, StringComparer.OrdinalIgnoreCase)
            .Where(g => !string.IsNullOrEmpty(g.Key))
            .ToList();

        int totalPatched = 0, totalFoldersFailed = 0;
        _logger.LogInformation("Bulk metadata patch: {Folders} case folders across {Files} files in library '{Library}'.",
            caseGroups.Count, records.Count, libraryTitle);

        foreach (var caseGroup in caseGroups)
        {
            var caseFolderRelPath = caseGroup.Key;
            // CaseId / CaseType are the same for every file in a case folder; any group member is fine.
            var rep = caseGroup.First();
            rep.Metadata.TryGetValue("CaseId", out var caseId);
            rep.Metadata.TryGetValue("CaseType", out var caseType);
            // DocumentId is per file. caseGroup.First() is often case_*_documents.xml (no DocumentId in metadata),
            // which caused every list item to skip DocumentId — only Wilberforce Case ID / Case Type were set.
            var documentIdByDestFileName = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var rec in caseGroup)
            {
                if (!rec.Metadata.TryGetValue("DocumentId", out var did) || string.IsNullOrWhiteSpace(did))
                    continue;
                var destName = Path.GetFileName((rec.MappedPath ?? string.Empty).Replace('\\', '/').TrimEnd('/'));
                if (string.IsNullOrWhiteSpace(destName))
                    destName = rec.Name;
                destName = CaseDocumentMetadataService.NormalizeFileNameForMetadataMatch(destName);
                documentIdByDestFileName[destName] = did.Trim();
            }

            if (string.IsNullOrWhiteSpace(caseId) && string.IsNullOrWhiteSpace(caseType) &&
                documentIdByDestFileName.Count == 0)
                continue;

            var fileServerRelativeUrls = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var rec in caseGroup)
            {
                var url = BuildServerRelativeFileUrlForCaseMetadataPatch(rootFolderUrl, yearPrefixToStrip, rec);
                if (!string.IsNullOrEmpty(url))
                    fileServerRelativeUrls.Add(url);
            }

            if (fileServerRelativeUrls.Count == 0)
            {
                _logger.LogWarning(
                    "Could not build server-relative file URL(s) for case folder '{Folder}' in library '{Library}'; metadata patch skipped for this group.",
                    caseFolderRelPath, libraryTitle);
                totalFoldersFailed++;
                continue;
            }

            try
            {
                var patched = await PatchCaseMetadataOnFilesByServerRelativeUrlAsync(
                        fileServerRelativeUrls,
                        caseIdField, caseId, caseTypeField, caseType, documentIdField, documentIdByDestFileName)
                    .ConfigureAwait(false);
                totalPatched += patched;
            }
            catch (Exception ex)
            {
                totalFoldersFailed++;
                _logger.LogWarning(ex, "Metadata patch failed for case folder '{Folder}' in library '{Library}'.",
                    caseFolderRelPath, libraryTitle);
            }
        }

        _logger.LogInformation("Bulk metadata patch complete for '{Library}': {Patched} items patched, {Failed} folder errors.",
            libraryTitle, totalPatched, totalFoldersFailed);
        return totalPatched;
    }

    private static string? BuildServerRelativeFileUrlForCaseMetadataPatch(
        string listRootUrl,
        string? yearPrefixToStrip,
        FileRecord record)
    {
        var rel = (record.MappedPath ?? string.Empty).Replace('\\', '/').Trim();
        if (string.IsNullOrEmpty(rel) || rel == "/")
        {
            if (string.IsNullOrWhiteSpace(record.Name))
                return null;
            rel = record.Name;
        }
        else
        {
            rel = rel.TrimStart('/');
        }

        if (!string.IsNullOrEmpty(yearPrefixToStrip))
        {
            var p = yearPrefixToStrip.TrimEnd('/') + "/";
            if (rel.StartsWith(p, StringComparison.OrdinalIgnoreCase))
                rel = rel[p.Length..];
        }

        rel = rel.Replace("//", "/").Trim().TrimStart('/');
        if (string.IsNullOrEmpty(rel))
            return null;

        return $"{listRootUrl.TrimEnd('/')}/{rel}";
    }

    /// <summary>
    /// Loads list items for known file URLs (avoids CAML on the whole list/folder) and applies metadata.
    /// </summary>
    private async Task<int> PatchCaseMetadataOnFilesByServerRelativeUrlAsync(
        IReadOnlyCollection<string> fileServerRelativeUrls,
        string? caseIdField, string? caseId,
        string? caseTypeField, string? caseType,
        string? documentIdField,
        IReadOnlyDictionary<string, string> documentIdByDestFileName)
    {
        // Smaller shell batches reduce pressure on large libraries (LVT can still hit wide GetFile batches).
        const int batchSize = 25;
        var urls = fileServerRelativeUrls.ToList();
        var patched = 0;

        for (var i = 0; i < urls.Count; i += batchSize)
        {
            var batch = urls.Skip(i).Take(batchSize).ToList();
            try
            {
                patched += await LoadAndSystemUpdateOneBatchAsync(
                        batch, caseIdField, caseId, caseTypeField, caseType, documentIdField, documentIdByDestFileName)
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                if (batch.Count <= 1)
                    throw;

                _logger.LogDebug(ex, "Batched file load for metadata patch failed; retrying {Count} file(s) individually.", batch.Count);
                foreach (var u in batch)
                {
                    try
                    {
                        patched += await LoadAndSystemUpdateOneBatchAsync(
                                new[] { u }, caseIdField, caseId, caseTypeField, caseType, documentIdField, documentIdByDestFileName)
                            .ConfigureAwait(false);
                    }
                    catch (Exception ex2)
                    {
                        _logger.LogWarning(ex2, "Metadata patch failed for file '{FileUrl}'.", u);
                    }
                }
            }
        }

        return patched;
    }

    private static bool IsListViewThresholdException(Exception ex)
    {
        for (var e = ex; e != null; e = e.InnerException)
        {
            if (e is ServerException se &&
                se.Message.Contains("list view threshold", StringComparison.OrdinalIgnoreCase))
                return true;
        }
        return false;
    }

    private async Task LoadListItemAllFieldsForFilesAsync(IReadOnlyList<SpFile> filesToLoad)
    {
        if (filesToLoad.Count == 0)
            return;

        const int chunk = 2;
        for (var i = 0; i < filesToLoad.Count;)
        {
            var remaining = filesToLoad.Count - i;
            var take = Math.Min(chunk, remaining);
            var slice = new List<SpFile>(take);
            for (var k = 0; k < take; k++)
                slice.Add(filesToLoad[i + k]);

            var loaded = await TryLoadListItemAllFieldsSliceAsync(slice).ConfigureAwait(false);
            if (loaded)
            {
                i += take;
                continue;
            }

            foreach (var f in slice)
            {
                if (await TryLoadListItemAllFieldsSingleAsync(f).ConfigureAwait(false))
                    continue;
            }

            i += take;
        }
    }

    private async Task<bool> TryLoadListItemAllFieldsSliceAsync(IReadOnlyList<SpFile> slice)
    {
        if (slice.Count == 0)
            return true;

        try
        {
            foreach (var f in slice)
                _clientContextG.Load(f, x => x.ListItemAllFields);
            await ExecuteQueryWithRetryAsync().ConfigureAwait(false);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex,
                "Batch list item load failed for {Count} file(s); will retry with smaller or single request.",
                slice.Count);
            return false;
        }
    }

    private async Task<bool> TryLoadListItemAllFieldsSingleAsync(SpFile f)
    {
        try
        {
            _clientContextG.Load(f, x => x.ListItemAllFields);
            await ExecuteQueryWithRetryAsync().ConfigureAwait(false);
            return true;
        }
        catch (Exception ex) when (IsListViewThresholdException(ex))
        {
            _logger.LogWarning(ex,
                "List view threshold: could not load list item for a file. Skipping this file in metadata patch.");
            return false;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not load list item for a file. Skipping this file in metadata patch.");
            return false;
        }
    }

    private async Task<int> LoadAndSystemUpdateOneBatchAsync(
        IReadOnlyList<string> fileServerRelativeUrls,
        string? caseIdField, string? caseId,
        string? caseTypeField, string? caseType,
        string? documentIdField,
        IReadOnlyDictionary<string, string> documentIdByDestFileName)
    {
        // Do not request ListItemAllFields with large multi-file GetFile requests in a single
        // Execute — that can still trip the list view threshold on very large document libraries.
        var files = new List<SpFile>(fileServerRelativeUrls.Count);
        foreach (var rel in fileServerRelativeUrls)
        {
            var f = _web.GetFileByServerRelativeUrl(rel);
            _clientContextG.Load(f, x => x.Exists, x => x.Name);
            files.Add(f);
        }

        try
        {
            await ExecuteQueryWithRetryAsync().ConfigureAwait(false);
        }
        catch (Exception ex) when (IsListViewThresholdException(ex) && fileServerRelativeUrls.Count > 1)
        {
            _logger.LogDebug(ex, "LVT loading file metadata shells in batch; retrying per URL.");
            return await LoadAndSystemUpdateOneFileAtATimeAsync(
                    fileServerRelativeUrls, caseIdField, caseId, caseTypeField, caseType, documentIdField, documentIdByDestFileName)
                .ConfigureAwait(false);
        }
        catch (Exception ex) when (IsListViewThresholdException(ex) && fileServerRelativeUrls.Count == 1)
        {
            _logger.LogDebug(ex, "LVT loading a single file shell; retrying with split load+update request.");
            return await LoadAndSystemUpdateOneFileAtATimeAsync(
                    fileServerRelativeUrls, caseIdField, caseId, caseTypeField, caseType, documentIdField, documentIdByDestFileName)
                .ConfigureAwait(false);
        }

        var existing = new List<SpFile>(files.Count);
        for (var j = 0; j < files.Count; j++)
        {
            var f = files[j];
            if (f.Exists)
            {
                existing.Add(f);
                continue;
            }

            _logger.LogDebug("File not found at server-relative path; metadata patch skipped: {FileUrl}.", fileServerRelativeUrls[j]);
        }

        if (existing.Count == 0)
            return 0;

        await LoadListItemAllFieldsForFilesAsync(existing).ConfigureAwait(false);

        var patched = 0;
        foreach (var f in existing)
        {
            var item = f.ListItemAllFields;
            if (item is null)
                continue;

            ApplyCaseMetadataToListItem(
                item, f.Name, caseIdField, caseId, caseTypeField, caseType, documentIdField, documentIdByDestFileName);
            patched++;
        }

        if (patched == 0)
            return 0;

        await ExecuteQueryWithRetryAsync().ConfigureAwait(false);
        return patched;
    }

    private async Task<int> LoadAndSystemUpdateOneFileAtATimeAsync(
        IReadOnlyList<string> fileServerRelativeUrls,
        string? caseIdField, string? caseId,
        string? caseTypeField, string? caseType,
        string? documentIdField,
        IReadOnlyDictionary<string, string> documentIdByDestFileName)
    {
        var patched = 0;
        foreach (var rel in fileServerRelativeUrls)
        {
            var f = _web.GetFileByServerRelativeUrl(rel);
            _clientContextG.Load(f, x => x.Exists, x => x.Name);
            try
            {
                await ExecuteQueryWithRetryAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Metadata patch: failed loading file info for '{FileUrl}'.", rel);
                continue;
            }

            if (!f.Exists)
                continue;

            _clientContextG.Load(f, x => x.ListItemAllFields);
            try
            {
                await ExecuteQueryWithRetryAsync().ConfigureAwait(false);
            }
            catch (Exception ex) when (IsListViewThresholdException(ex))
            {
                _logger.LogWarning(ex, "Metadata patch: list view threshold loading list item for '{FileUrl}'.", rel);
                continue;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Metadata patch: failed loading list item for '{FileUrl}'.", rel);
                continue;
            }

            if (f.ListItemAllFields is null)
                continue;

            ApplyCaseMetadataToListItem(
                f.ListItemAllFields, f.Name, caseIdField, caseId, caseTypeField, caseType, documentIdField, documentIdByDestFileName);
            try
            {
                await ExecuteQueryWithRetryAsync().ConfigureAwait(false);
                patched++;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Metadata patch SystemUpdate failed for file '{FileUrl}'.", rel);
            }
        }

        return patched;
    }

    private static void ApplyCaseMetadataToListItem(
        ListItem item,
        string? fileName,
        string? caseIdField, string? caseId,
        string? caseTypeField, string? caseType,
        string? documentIdField,
        IReadOnlyDictionary<string, string> documentIdByDestFileName)
    {
        if (!string.IsNullOrWhiteSpace(caseIdField) && !string.IsNullOrWhiteSpace(caseId))
            item[caseIdField] = caseId;
        if (!string.IsNullOrWhiteSpace(caseTypeField) && !string.IsNullOrWhiteSpace(caseType))
            item[caseTypeField] = caseType;
        if (!string.IsNullOrWhiteSpace(documentIdField) && documentIdByDestFileName.Count > 0)
        {
            var spLookupName = CaseDocumentMetadataService.NormalizeFileNameForMetadataMatch(fileName);
            if (!string.IsNullOrWhiteSpace(spLookupName) &&
                documentIdByDestFileName.TryGetValue(spLookupName, out var perFileDocId) &&
                !string.IsNullOrWhiteSpace(perFileDocId))
                item[documentIdField] = perFileDocId;
        }

        item.SystemUpdate();
    }
}