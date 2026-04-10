namespace BlobToSharePointMigrator.Models;

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
