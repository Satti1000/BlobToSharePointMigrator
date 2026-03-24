namespace BlobToSharePointMigrator.Models;

public class FolderMapping
{
    public string Source      { get; set; } = string.Empty;
    public string Destination { get; set; } = string.Empty;
}

public class MappingConfig
{
    public List<FolderMapping> FolderMappings { get; set; } = new();
}
