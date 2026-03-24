namespace BlobToSharePointMigrator.Models;

public class FileRecord
{
    public string Name           { get; set; } = string.Empty;
    public string BlobPath       { get; set; } = string.Empty;
    public string MappedPath     { get; set; } = string.Empty;
    public long   SizeBytes      { get; set; }
    public string ContentType    { get; set; } = string.Empty;
    public string LastModified   { get; set; } = string.Empty;
    public string CreatedOn      { get; set; } = string.Empty;
    public bool   IsAllowed      { get; set; }
    public string SkipReason     { get; set; } = string.Empty;
    public IDictionary<string, string> Metadata { get; set; } = new Dictionary<string, string>();
}

public class MigrationResult
{
    public string SourceFile     { get; set; } = string.Empty;
    public string DestPath       { get; set; } = string.Empty;
    public long   SizeBytes      { get; set; }
    public string LastModified   { get; set; } = string.Empty;
    public string Status         { get; set; } = string.Empty;
    public string SharePointUrl  { get; set; } = string.Empty;
    public string Error          { get; set; } = string.Empty;
    public string Duration       { get; set; } = string.Empty;
}

public class FolderMapping
{
    public string Source      { get; set; } = string.Empty;
    public string Destination { get; set; } = string.Empty;
}

public class MappingConfig
{
    public List<FolderMapping> FolderMappings { get; set; } = new();
}

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
    public List<string> Errors { get; set; } = new();
}
