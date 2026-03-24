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
