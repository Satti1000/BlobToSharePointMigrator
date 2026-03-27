using BlobToSharePointMigrator.Configuration;
using BlobToSharePointMigrator.Models;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Globalization;
using CsvHelper;

namespace BlobToSharePointMigrator.Services;

public class ReportService
{
    private readonly MigrationSettings _settings;
    private readonly ILogger<ReportService> _logger;
    private Dictionary<string, string> _deltaTracking = new();

    public ReportService(MigrationSettings settings, ILogger<ReportService> logger)
    {
        _settings = settings;
        _logger   = logger;
    }

    public void LoadDeltaTracking()
    {
        if (!_settings.DeltaMode || !File.Exists(_settings.DeltaTrackingFile))
            return;

        var json = File.ReadAllText(_settings.DeltaTrackingFile);
        _deltaTracking = JsonConvert.DeserializeObject<Dictionary<string, string>>(json)
            ?? new Dictionary<string, string>();

        _logger.LogInformation("Delta mode: {Count} previously migrated files loaded", _deltaTracking.Count);
    }

    public bool ShouldSkip(FileRecord record)
    {
        if (!_settings.DeltaMode) return false;
        return _deltaTracking.TryGetValue(record.BlobPath, out var lastMod)
            && lastMod == record.LastModified;
    }

    public void TrackMigrated(FileRecord record)
    {
        _deltaTracking[record.BlobPath] = record.LastModified;
    }

    public void SaveDeltaTracking()
    {
        var json = JsonConvert.SerializeObject(_deltaTracking, Formatting.Indented);
        File.WriteAllText(_settings.DeltaTrackingFile, json);
    }

    public void WriteReport(List<MigrationResult> results)
    {
        using var writer = new StreamWriter(_settings.ReportFile);
        using var csv    = new CsvWriter(writer, CultureInfo.InvariantCulture);
        csv.WriteRecords(results);
        _logger.LogInformation("Report saved: {File}", _settings.ReportFile);
    }

    public void PrintSummary(List<MigrationResult> results, List<FileRecord> skipped)
    {
        var success  = results.Count(r => r.Status == "Success");
        var partial  = results.Count(r => r.Status == "PartialSuccess");
        var failed   = results.Count(r => r.Status == "Failed");

        Console.WriteLine();
        Console.WriteLine("===================================================");
        Console.WriteLine("   MIGRATION COMPLETE");
        Console.WriteLine("===================================================");
        Console.WriteLine($"   Succeeded  : {success}");
        Console.WriteLine($"   Partial    : {partial}");
        Console.WriteLine($"   Failed     : {failed}");
        Console.WriteLine($"   Skipped    : {skipped.Count}");
        Console.WriteLine($"   Report     : {_settings.ReportFile}");
        Console.WriteLine("===================================================");
        Console.WriteLine();

        if (results.Any())
        {
            Console.WriteLine($"{"Source File",-35} {"Destination",-40} {"Status",-10} {"Size",10}");
            Console.WriteLine(new string('-', 100));
            foreach (var r in results)
                Console.WriteLine($"{r.SourceFile,-35} {r.DestPath,-40} {r.Status,-10} {r.SizeBytes,10}");
        }

        if (failed > 0)
        {
            Console.WriteLine();
            Console.WriteLine("Failures:");
            foreach (var r in results.Where(r => r.Status == "Failed"))
                Console.WriteLine($"  - {r.SourceFile}: {r.Error}");
        }
    }
}
