using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using BlobToSharePointMigrator.Configuration;
using BlobToSharePointMigrator.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace BlobToSharePointMigrator.Services;

public class BlobInventoryService
{
    private readonly MigrationSettings _settings;
    private IConfigurationSection _processFlags;
    private readonly ILogger<BlobInventoryService> _logger;
    private readonly BlobServiceClient _blobServiceClient;

    public BlobInventoryService(IConfigurationSection processFlags, MigrationSettings settings, ILogger<BlobInventoryService> logger)
    {
        _settings = settings;
        _logger = logger;
        _blobServiceClient = new BlobServiceClient(settings.BlobConnectionString);
        _processFlags = processFlags;
    }

    public async Task<List<FileRecord>> InventoryAsync()
    {
        _logger.LogInformation("Connecting to Azure Blob Storage...");
        var containerClient = _blobServiceClient.GetBlobContainerClient(_settings.SourceContainer);

        _logger.LogInformation("Inventorying container: {Container}", _settings.SourceContainer);

        var records = new List<FileRecord>();
        var allowedExtensions = new HashSet<string>(_settings.AllowedExtensions, StringComparer.OrdinalIgnoreCase);
        var blobFolderPrefix = _processFlags.GetSection("BlobFolderPrefix").Value;
        var prefix = blobFolderPrefix?.TrimEnd('/') ?? "";
        var listPrefix = string.IsNullOrEmpty(prefix) ? "" : prefix + "/";

        await foreach (BlobItem blob in containerClient.GetBlobsAsync(prefix: listPrefix)) //BlobTraits.Metadata | BlobTraits.Tags))
        {
            var ext = Path.GetExtension(blob.Name).ToLower();
            var allowed = allowedExtensions.Contains(ext);
            var skipReason = allowed ? string.Empty : $"Extension '{ext}' not in allowed list";

            var record = new FileRecord
            {
                Name = Path.GetFileName(blob.Name),
                BlobPath = blob.Name,
                SizeBytes = blob.Properties.ContentLength ?? 0,
                ContentType = blob.Properties.ContentType ?? "application/octet-stream",
                LastModified = blob.Properties.LastModified?.ToString("yyyy-MM-dd HH:mm:ss") ?? string.Empty,
                CreatedOn = blob.Properties.CreatedOn?.ToString("yyyy-MM-dd HH:mm:ss") ?? string.Empty,
                IsAllowed = allowed,
                SkipReason = skipReason,
                Metadata = blob.Metadata ?? new Dictionary<string, string>()
            };

            records.Add(record);
        }

        _logger.LogInformation("Inventory complete. Total: {Total}, Allowed: {Allowed}, Skipped: {Skipped}",
            records.Count,
            records.Count(r => r.IsAllowed),
            records.Count(r => !r.IsAllowed));

        return records;
    }

    public async Task<Stream> DownloadBlobAsync(string blobPath)
    {
        var containerClient = _blobServiceClient.GetBlobContainerClient(_settings.SourceContainer);
        var blobClient = containerClient.GetBlobClient(blobPath);
        var response = await blobClient.DownloadStreamingAsync();
        return response.Value.Content;
    }
}
