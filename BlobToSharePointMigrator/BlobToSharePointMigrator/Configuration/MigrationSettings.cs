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
    public int UploadParallelism            { get; set; } = 8;
    public int MaxFilesToMigrate            { get; set; } = 0;
    public int JobPollIntervalSeconds       { get; set; } = 10;
    public int JobTimeoutMinutes            { get; set; } = 60;
    public string SharePointTargetFolder    { get; set; } = string.Empty; // optional extra folder root under the library
    public int CsomRequestTimeoutSeconds    { get; set; } = 600;
    public bool UseYyyyCaseNumberPath       { get; set; } = true;
    public int MaxParallelJobs              { get; set; } = 3;
    public Dictionary<string,string> MetadataFieldMap { get; set; } = new(); // blobMetaKey -> sharepointFieldInternalName
    public string BlobFolderPrefix          { get; set; } = string.Empty; // optional source filter
    public int MigrationYear                { get; set; } = 0;            // 0 = all years
    public string FailedItemsFile           { get; set; } = "failed-files.csv";
    public bool RetryFailedOnly             { get; set; } = false;
    public bool RetryIncludeAlreadyExists   { get; set; } = false;
}
