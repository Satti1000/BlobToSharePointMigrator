using System.Text.RegularExpressions;

namespace BlobToSharePointMigrator.Services;

/// <summary>
/// Only treat <c>{caseNumber}/{caseNumber}_Documents/...</c> as a case-documents path (same number in
/// both segments). Paths like <c>335074/122244_Documents</c> are excluded — they caused duplicate/wrong
/// inventory in the loose <c>*_Documents</c> matcher.
/// </summary>
internal static class CaseDocumentsPathRules
{
    private static readonly Regex DocumentsFolderSegment = new(@"^(\d+)_Documents$", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    internal static string[] SplitPathSegments(string? blobPath)
    {
        var normalized = (blobPath ?? string.Empty).Replace('\\', '/').Trim('/');
        if (string.IsNullOrWhiteSpace(normalized))
            return Array.Empty<string>();

        return normalized.Split('/', StringSplitOptions.RemoveEmptyEntries);
    }

    /// <summary>
    /// Index of the first segment matching <c>{n}_Documents</c> whose parent segment equals <c>n</c>, or -1.
    /// </summary>
    internal static int FindAlignedDocumentsSegmentIndex(string[] segments)
    {
        for (var i = 0; i < segments.Length; i++)
        {
            var m = DocumentsFolderSegment.Match(segments[i]);
            if (!m.Success || i == 0)
                continue;

            if (string.Equals(segments[i - 1], m.Groups[1].Value, StringComparison.OrdinalIgnoreCase))
                return i;
        }

        return -1;
    }

    internal static bool IsUnderAlignedCaseNumberDocuments(string? blobPath) =>
        FindAlignedDocumentsSegmentIndex(SplitPathSegments(blobPath)) >= 0;

    /// <summary>Case number from the first aligned <c>{n}_Documents</c> segment, or null.</summary>
    internal static string? TryGetAlignedCaseNumber(string? blobPath)
    {
        var segments = SplitPathSegments(blobPath);
        var i = FindAlignedDocumentsSegmentIndex(segments);
        if (i < 0)
            return null;

        var m = DocumentsFolderSegment.Match(segments[i]);
        return m.Success ? m.Groups[1].Value : null;
    }
}
