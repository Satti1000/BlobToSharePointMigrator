namespace BlobToSharePointMigrator.Models;

// SharePoint Migration Job models
public class MigrationJobInfo
{
    public Guid JobId { get; set; }
    public string Status { get; set; } = string.Empty;
    public int Progress { get; set; }
    public string CreatedDateTime { get; set; } = string.Empty;
    public int TotalFileCount { get; set; }
    public int ProcessedFileCount { get; set; }
    public int FailedFileCount { get; set; }
    public int AlreadyExistsCount { get; set; }
    public int OtherErrorCount { get; set; }
    public List<string> Errors { get; set; } = new();
}
