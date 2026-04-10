# Blob-to-SharePoint ETL Migration Pipeline

A .NET 8 C# console application that migrates documents from **Azure Blob Storage** to **SharePoint Online** using the official **SharePoint Migration API** — no Graph API, no manual uploads.

## Architecture

```
AZURE BLOB STORAGE          MIGRATION APP (.NET 8)        SHAREPOINT ONLINE
- Documents container   ->  - Inventory & filter     ->  - Document Library
- Metadata tags             - Extract metadata            - Custom metadata
- File properties           - Transform paths             - Preserved integrity
                             - Upload via SP API
                             - Log and report
```

## Features

- Reads directly from existing Azure Blob container
- Filters by file type (PDF, CSV, HTML; no images/video)
- Transforms folder structure via configurable mapping table
- Batch uploads via SharePoint Migration API (SPMI) with reduced throttling impact
- Handles 5000+ files without per-request upload loops
- Preserves blob metadata as SharePoint column values
- Full progress logging and CSV confirmation report
- Delta reload support; reruns skip unchanged files
- App-only authentication via certificate-based OAuth (Tenant ID + Client ID + Certificate)
- Deployable as Azure Container Instance (up to 3 hours runtime)
- Serilog rolling file logs for detailed run diagnostics

## Requirements

- .NET 8 SDK
- Azure Blob Storage account
- SharePoint Online site with a Document Library
- Azure App Registration with SharePoint permissions

## Setup

### 1. App Registration (Azure Portal)

1. Go to **Microsoft Entra ID** -> **App registrations** -> **New registration**
2. Name it `BlobToSharePointMigrator`
3. Note the **Tenant ID**, **Client ID**
4. Go to **Certificates & secrets** -> **Certificates** tab -> **Upload certificate**
   - Upload your `.pfx` certificate file
   - Note the **Thumbprint**
5. Go to **API permissions** -> **Add permission**:
   - Select **SharePoint** -> **Application permissions**
   - Add `Sites.FullControl.All`
6. Click **Grant admin consent**

Note: `Sites.FullControl.All` is what this implementation is currently tested with. Permissions can be reduced later after security review and validation.

### 2. Configure appsettings.json

Copy `appsettings.template.json` to `appsettings.json` and fill in your values:

```json
{
  "Migration": {
    "BlobConnectionString":        "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net",
    "SourceContainer":             "your-container-name",
    "SharePointTenantId":          "your-tenant-id",
    "SharePointClientId":          "your-app-id",
    "SharePointCertificatePath":   "C:\\path\\to\\certificate.pfx",
    "SharePointCertificatePassword": "your-cert-password",
    "SharePointCertificateThumbprint": "thumbprint-from-azure-portal",
    "SharePointSiteUrl":           "https://yourtenant.sharepoint.com",
    "SharePointDocumentLibrary":   "Shared Documents",
    "AllowedExtensions":           [".pdf", ".csv", ".html"],
    "DeltaMode":                   false,
    "UseYyyyCaseNumberPath":       true
  }
}
```

IMPORTANT: `appsettings.json` is in `.gitignore`; never commit credentials. Use `appsettings.template.json` as reference.

### 3. Path rule (primary mode)

Primary destination naming now follows DynamicETL rule:

- Source pattern contains a `YYYY` segment and `<CaseNumber>_Documents`
- Destination becomes: `YYYY/<CaseNumber>/<everything after _Documents>`
- The `YYYY` value is taken from the year segment in the source blob path format used by this migration.

Example:

- Source blob  
  `Wilerforce/Final_InactiveCases_01Feb2026_doc/2025/MXX/NRM_Cases_2377/CaseNumberFolder/23771_Documents/EVERYTHING/a.xml`
- Destination path  
  `2025/23771/EVERYTHING/a.xml`

When the pattern is not found, the app falls back to `mapping.json` mapping.

### 4. Configure mapping.json

```json
{
  "folderMappings": [
    { "source": "hr/contracts",  "destination": "HR/Employment-Contracts" },
    { "source": "finance",       "destination": "Finance" },
    { "source": "",              "destination": "General" }
  ]
}
```

## Run

```bash
# Full migration
dotnet run

# Delta reload (skip unchanged files)
# Set DeltaMode: true in appsettings.json
dotnet run
```

## Output

| File | Description |
|------|-------------|
| `logs/etl-YYYYMMDD.log` | Serilog rolling daily log (primary detailed log) |
| `migration-report.csv` | Per-file results with SharePoint URLs |
| `migrated-files.json`  | Delta tracking state |

Log path and retention are controlled in `appsettings.json` under `Serilog:WriteTo`.

## SharePoint Migration API (SPMI) - What Changed?

**Latest refactoring (2026):** The upload layer has been rebuilt to use SharePoint Migration API instead of per-file REST uploads.

### Performance Impact (reference only)

| Scenario | Before (REST API) | After (SPMI) | Typical improvement |
| --- | --- | --- | --- |
| 100 files | 5 minutes | 1 minute | About 5x faster |
| 1,000 files | 1 hour | 5 minutes | About 12x faster |
| 5,000 files | Throttled (not reliable) | 25 minutes | Works in batch mode |

These numbers are examples from test runs. Actual duration depends on tenant load, SharePoint backend processing time, source file mix, and network throughput.

### Key Benefits

- **Reduced throttling risk** - Bypasses per-request API limits by batching
- **Async processing** - SharePoint processes files on their backend
- **Scalable** - Handles very large file sets
- **Reliable** - Single job submission vs. thousands of individual calls

### What's Different?

**Old approach (per-file):**

```
For each of 5000 files:
  -> Create folder (if needed)
  -> Upload file via REST API
  -> Wait for response
Result: Throttled after ~500 files
```

**New approach (batch via SPMI):**

```
Generate manifest of all 5000 files
-> Submit migration job (1 API call)
-> SharePoint processes asynchronously
-> Poll for completion
Result: All 5000 files done in 20-30 minutes
```

### For Existing Users

**Your migration still works!** Just:

1. Pull latest code
2. Run as normal: `dotnet run`
3. Application handles the rest

Delta mode automatically skips already-migrated files, so re-runs are safe.

### Documentation

- **[SPMI_QUICKSTART.md](./SPMI_QUICKSTART.md)** — Fast reference and troubleshooting
- **[MIGRATION_API_UPGRADE.md](./MIGRATION_API_UPGRADE.md)** — Full architectural guide
- **[SPMI_TECHNICAL_GUIDE.md](./SPMI_TECHNICAL_GUIDE.md)** — Deep technical details for developers

## Docker / Azure Container Instance

```bash
docker build -t blob2sharepoint .
docker run -v $(pwd)/appsettings.json:/app/appsettings.json blob2sharepoint
```
