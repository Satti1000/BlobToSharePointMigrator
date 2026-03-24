namespace BlobToSharePointMigrator.Configuration;

public class MigrationSettings
{
    public string BlobConnectionString      { get; set; } = string.Empty;
    public string SourceContainer           { get; set; } = string.Empty;
    public string SharePointTenantId        { get; set; } = string.Empty;
    public string SharePointClientId        { get; set; } = string.Empty;
    public string SharePointClientSecret    { get; set; } = string.Empty;
    public string SharePointSiteUrl         { get; set; } = string.Empty;
    public string SharePointDocumentLibrary { get; set; } = "Documents";
    public List<string> AllowedExtensions   { get; set; } = new() { ".pdf", ".csv", ".html", ".txt", ".xml" };
    public string MappingFile               { get; set; } = "mapping.json";
    public string LogFile                   { get; set; } = "migration-log.txt";
    public string ReportFile                { get; set; } = "migration-report.csv";
    public string DeltaTrackingFile         { get; set; } = "migrated-files.json";
    public bool   DeltaMode                 { get; set; } = false;
	public string SharePointCertificatePath        { get; set; } = string.Empty;
	public string SharePointCertificatePassword    { get; set; } = string.Empty;
	public string SharePointCertificateThumbprint  { get; set; } = string.Empty;
}
