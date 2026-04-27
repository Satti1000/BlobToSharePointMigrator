namespace BlobToSharePointMigrator.Configuration;

public class MigrationSettings
{
    public string BlobConnectionString      { get; set; } = string.Empty;
    public string SourceContainer           { get; set; } = string.Empty;
    public string SharePointTenantId        { get; set; } = string.Empty;
    public string SharePointClientId        { get; set; } = string.Empty;
    public string SharePointClientSecret    { get; set; } = string.Empty;
    public string SharePointSiteUrl         { get; set; } = string.Empty;
    /// <summary>SharePoint admin center URL; used to derive tenant name for logging (e.g. https://contoso-admin.sharepoint.com).</summary>
    public string AdminUrl                  { get; set; } = string.Empty;
    /// <summary>Document library column display names for CaseId / CaseType / DocumentId (field resolution in SharePoint).</summary>
    public string SharePointCaseIdColumnDisplayName     { get; set; } = string.Empty;
    public string SharePointCaseTypeColumnDisplayName   { get; set; } = string.Empty;
    public string SharePointDocumentIdColumnDisplayName { get; set; } = string.Empty;
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
    public bool YearAsLibrary               { get; set; } = true; // when true, target library title is the year (YYYY)
    public int MaxParallelJobs              { get; set; } = 3;
    public bool EnableMigrationJobSaveConflictRetry { get; set; } = false;
    public int MigrationJobSaveConflictRetries { get; set; } = 1;
    public int MigrationJobSaveConflictRetryDelaySeconds { get; set; } = 45;

    /// <summary>
    /// When true, a batch that finishes with <c>CompletedWithErrors</c> and SPMI <c>FilesCreated</c> below the
    /// submitted file count (incomplete vs queue counter) is still marked <c>PartialSuccess</c> so STEP 5 can
    /// refresh CaseId / CaseType / DocumentId on destination paths. Use for reruns where SharePoint may have
    /// updated or skipped objects without matching the queue "created" count. Not used when the job status is
    /// <c>Failed</c> or when Save Conflict diagnostics are present (those batches stay all-Failed).
    /// </summary>
    public bool TreatIncompleteSpmiBatchAsPartialSuccessForMetadata { get; set; } = false;

    public Dictionary<string,string> MetadataFieldMap { get; set; } = new(); // blobMetaKey -> sharepointFieldInternalName
    public string BlobFolderPrefix          { get; set; } = string.Empty; // optional source filter
    public int MigrationYear                { get; set; } = 0;            // 0 = all years
    public string FailedItemsFile           { get; set; } = "failed-files.csv";
    public bool RetryFailedOnly             { get; set; } = false;
    public bool RetryIncludeAlreadyExists   { get; set; } = false;

    /// <summary>
    /// When true, SPMI queue messages containing "already exists" are logged and summarized as
    /// overwrite/replace intent (client wording: "already exists and overwritten").
    /// </summary>
    public bool ReportExistingFilesAsOverwritten { get; set; } = true;

    /// <summary>
    /// When true, loads <c>case_{caseNumber}_documents.xml</c> under each <c>*_Documents</c> case folder,
    /// matches each file to manifest <c>Document</c> entries (exact / normalized / MIME-typed / extensionless stem),
    /// and sets record <c>Metadata["DocumentId"]</c> (plus folder-level entries for email-style folder documents).
    /// </summary>
    public bool AssignDocumentIdFromCaseXml { get; set; } = true;

    /// <summary>
    /// When true, logs one Information line per file immediately after it is uploaded into the
    /// encrypted migration package (staging). SharePoint still applies the package asynchronously
    /// via the migration job; set false for very large jobs to reduce log volume.
    /// </summary>
    public bool LogPerFileCaseProgress { get; set; } = true;
}
