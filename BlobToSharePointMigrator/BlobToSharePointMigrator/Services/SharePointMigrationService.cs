using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Queues;
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
using System.Security.Cryptography.X509Certificates;
using System.Xml.Linq;

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

    public SharePointMigrationService(IConfigurationSection processFlags, MigrationSettings settings, ILogger<SharePointMigrationService> logger)
    {
        _settings = settings;
        _logger = logger;
        _processFlags = processFlags;
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

            // Resolve the target document library
            var list = _web.Lists.GetByTitle(_settings.SharePointDocumentLibrary);
            _clientContextG.Load(list, l => l.Id, l => l.RootFolder);
            _clientContextG.Load(list.RootFolder, f => f.UniqueId, f => f.ServerRelativeUrl);
            await ExecuteQueryWithRetryAsync().ConfigureAwait(false);

            _listId = list.Id.ToString();
            _rootFolderId = list.RootFolder.UniqueId.ToString();

            // Normalize to web-relative library root for SPMI manifest URLs.
            // Example:
            //   list.RootFolder.ServerRelativeUrl = "sites/sharepointmigration/Shared Documents"
            //   _web.ServerRelativeUrl            = "sites/sharepointmigration"
            //   _rootFolderUrl                    = "Shared Documents"
            // Without this, SharePoint import can duplicate site path and fail:
            //   ".../sites/sharepointmigration/sites/sharepointmigration/Shared Documents/General"
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

            _logger.LogInformation("Target library: {Library} (List ID: {ListId}, Root URL: {RootUrl})",
                _settings.SharePointDocumentLibrary, _listId, _rootFolderUrl);
        }
        catch (Exception ex)
        {
            _logger.LogError("Error reading Target library: {Library})", ex.Message);
            throw;
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
        Func<string, Task<Stream>> blobDownloader)
    {
        if (records.Count == 0)
            throw new ArgumentException("No files to migrate");

        _logger.LogInformation("Preparing SPMI migration package for {Count} files...", records.Count);

        // Pre-validate and normalize mapped paths. Bad paths are skipped so one invalid item
        // does not fail the full migration batch.
        var validRecords = new List<FileRecord>(records.Count);
        var skippedInvalidPath = 0;
        foreach (var record in records)
        {
            try
            {
                var mappedPath = record.MappedPath.Replace('\\', '/').Trim('/');
                if (string.IsNullOrWhiteSpace(mappedPath))
                    throw new InvalidOperationException("Mapped path is empty.");

                if (PathTransformService.ContainsInvalidSharePointChars(mappedPath))
                    throw new InvalidOperationException($"Mapped path contains invalid SharePoint characters: {mappedPath}");

                var safePath = PathTransformService.SanitizeSharePointRelativePath(mappedPath);
                if (string.IsNullOrWhiteSpace(safePath))
                    throw new InvalidOperationException("Mapped path became empty after sanitization.");

                if (safePath.Length > 400)
                    throw new InvalidOperationException($"Mapped path exceeds supported length: {safePath.Length}");

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

        _logger.LogInformation("Data container provisioned: {Uri}", dataContainerUri.Split('?')[0]);
        _logger.LogInformation("Metadata container provisioned: {Uri}", metadataContainerUri.Split('?')[0]);
        _queueUri = queueUri;
        _logger.LogInformation("Report queue provisioned.");

        // Step 2: Upload source files to the data container (AES-encrypted)
        _logger.LogInformation("Uploading {Count} source files (encrypted) to SharePoint data container...", validRecords.Count);

        var dataContainer = new BlobContainerClient(new Uri(dataContainerUri));
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

        var metadataContainer = new BlobContainerClient(new Uri(metadataContainerUri));

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
            dataContainerUri,
            metadataContainerUri,
            queueUri,
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
        int pollIntervalMs = 10000)
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
                _logger.LogInformation("Job {JobId} returned State=None. Reading queue report...", jobId);
                var queueSummary = await ReadQueueReportAsync(jobId);

                if (queueSummary.Status is "Completed" or "CompletedWithErrors" or "Failed")
                {
                    _logger.LogInformation("Migration job {JobId} finalized with queueStatus={Status}.", jobId, queueSummary.Status);
                    return new MigrationJobInfo
                    {
                        JobId = jobId,
                        Status = queueSummary.Status,
                        Progress = 100,
                        CreatedDateTime = DateTime.UtcNow.ToString("O"),
                        ProcessedFileCount = queueSummary.FilesCreated,
                        FailedFileCount = queueSummary.TotalErrors,
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
    /// Reads migration job report messages from the Azure Queue to get actual success/failure details.
    /// </summary>
    private async Task<QueueSummary> ReadQueueReportAsync(Guid jobId)
    {
        var summary = new QueueSummary();

        if (string.IsNullOrEmpty(_queueUri))
            return summary;

        var queueClient = new QueueClient(new Uri(_queueUri));
        Azure.Response<Azure.Storage.Queues.Models.QueueMessage[]> messages;
        try
        {
            messages = await queueClient.ReceiveMessagesAsync(maxMessages: 32);
        }
        catch (Exception ex) when (IsTransientRequestError(ex))
        {
            _logger.LogWarning(ex, "Transient queue-read error while checking job {JobId}.", jobId);
            return summary;
        }

        foreach (var msg in messages.Value)
        {
            try
            {
                var outerBody = msg.Body.ToString();
                var decrypted = DecryptQueueMessage(outerBody);
                _logger.LogInformation("Queue report: {Message}", decrypted);

                var json = Newtonsoft.Json.Linq.JObject.Parse(decrypted);
                var eventType = json["Event"]?.ToString() ?? "";
                var message = json["Message"]?.ToString()
                           ?? json["ErrorMessage"]?.ToString() ?? "";
                var jobIdInMessage = json["JobId"]?.ToString();

                // Best-effort filter to only process messages for our job.
                if (!string.IsNullOrWhiteSpace(jobIdInMessage) &&
                    !string.Equals(jobIdInMessage, jobId.ToString(), StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                if (eventType.Contains("JobEnd", StringComparison.OrdinalIgnoreCase) ||
                    eventType.Contains("JobError", StringComparison.OrdinalIgnoreCase) ||
                    decrypted.Contains("JobEnd", StringComparison.OrdinalIgnoreCase) ||
                    decrypted.Contains("JobError", StringComparison.OrdinalIgnoreCase))
                {
                    summary.SawFinalEvent = true;
                }

                if (eventType.Contains("JobFatalError", StringComparison.OrdinalIgnoreCase))
                {
                    summary.SawFatalError = true;
                }

                if (eventType.Contains("Error", StringComparison.OrdinalIgnoreCase)
                    || eventType.Contains("Fail", StringComparison.OrdinalIgnoreCase)
                    || eventType.Contains("Warning", StringComparison.OrdinalIgnoreCase))
                {
                    summary.Errors.Add($"[{eventType}] {message}");
                    _logger.LogWarning("Migration issue: [{Event}] {Message}", eventType, message);
                }

                if (eventType.Contains("JobEnd", StringComparison.OrdinalIgnoreCase))
                {
                    summary.FilesCreated = ParseInt(json["FilesCreated"]?.ToString());
                    summary.TotalErrors = ParseInt(json["TotalErrors"]?.ToString());
                }
            }
            catch (Exception ex)
            {
                _logger.LogDebug("Could not parse queue message: {Error}", ex.Message);
            }
        }

        if (summary.SawFinalEvent)
        {
            if (summary.SawFatalError || summary.FilesCreated == 0)
                summary.Status = "Failed";
            else if (summary.Errors.Count > 0 || summary.TotalErrors > 0)
                summary.Status = "CompletedWithErrors";
            else
                summary.Status = "Completed";
        }

        return summary;
    }

    private sealed class QueueSummary
    {
        public string Status { get; set; } = "InProgress";
        public List<string> Errors { get; } = new();
        public bool SawFinalEvent { get; set; }
        public bool SawFatalError { get; set; }
        public int FilesCreated { get; set; }
        public int TotalErrors { get; set; }
    }

    private static int ParseInt(string? value)
    {
        return int.TryParse(value, out var parsed) ? parsed : 0;
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
                new XAttribute("IncludeSecurity", "None"),
                new XAttribute("IncludeVersions", "LastMajor"),
                new XAttribute("ExportMethod", "ExportAll"))));
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

                    root.Add(new XElement(ns + "SPObject",
                        new XAttribute("Id", folderId),
                        new XAttribute("ObjectType", "SPFolder"),
                        new XAttribute("ParentId", parentFolderId),
                        new XAttribute("ParentWebId", _webId),
                        new XAttribute("ParentWebUrl", webUrl),
                        new XElement(ns + "Folder",
                            new XAttribute("Id", folderId),
                            new XAttribute("Url", $"{_rootFolderUrl}/{folderAccumulator}"),
                            new XAttribute("ParentFolderId", parentFolderId),
                            new XAttribute("ParentWebId", _webId),
                            new XAttribute("ParentWebUrl", webUrl),
                            new XAttribute("Name", parts[i]))));
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

            // Optional: include metadata as Properties under File when explicitly enabled and mapped
            if (record.Metadata is { Count: > 0 })
            {
                var properties = new XElement(ns + "Properties");
                foreach (var kvp in record.Metadata)
                {
                    if (string.IsNullOrWhiteSpace(kvp.Key)) continue;
                    if (!_settings.MetadataFieldMap.TryGetValue(kvp.Key, out var fieldInternalName) ||
                        string.IsNullOrWhiteSpace(fieldInternalName))
                    {
                        continue; // only mapped keys are emitted
                    }
                    var value = kvp.Value ?? string.Empty;
                    properties.Add(new XElement(ns + "Property",
                        new XAttribute("Name", fieldInternalName),
                        value));
                }

                if (properties.HasElements)
                {
                    fileElement.Element(ns + "File")!.Add(properties);
                }
            }

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
}