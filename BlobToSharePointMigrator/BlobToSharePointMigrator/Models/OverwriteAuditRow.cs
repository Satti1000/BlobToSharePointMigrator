namespace BlobToSharePointMigrator.Models;

public class OverwriteAuditRow
{
    public string SourceFile { get; set; } = string.Empty;
    public string DestPath { get; set; } = string.Empty;
    public string CaseFolder { get; set; } = string.Empty;
    public string Year { get; set; } = string.Empty;
    public string Status { get; set; } = string.Empty;
    public bool DocumentIdPresent { get; set; }
    public bool MetadataPatchEligible { get; set; }
    public bool BatchAlreadyExistsSignal { get; set; }
    public string BatchJobStatus { get; set; } = string.Empty;
    public int BatchFilesCreated { get; set; }
    public int BatchSubmittedCount { get; set; }
    public string Notes { get; set; } = string.Empty;
}
