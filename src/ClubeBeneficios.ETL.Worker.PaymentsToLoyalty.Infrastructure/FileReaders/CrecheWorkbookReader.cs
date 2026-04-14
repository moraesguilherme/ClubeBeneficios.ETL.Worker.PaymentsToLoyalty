using ClosedXML.Excel;

namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.FileReaders;

public class CrecheWorkbookReader : ICrecheWorkbookReader
{
    private static readonly HashSet<string> IgnoredSheets = new(StringComparer.OrdinalIgnoreCase)
    {
        "LISTA APOIO",
        "BRINDES",
        "PLANOS HOTEL",
        "TURMA"
    };

    public bool CanRead(string filePath)
    {
        return filePath.EndsWith(".xlsx", StringComparison.OrdinalIgnoreCase)
            && Path.GetFileName(filePath).Contains("CRECHE", StringComparison.OrdinalIgnoreCase);
    }

    public IReadOnlyList<IXLWorksheet> GetMonthlyWorksheets(XLWorkbook workbook)
    {
        return workbook.Worksheets
            .Where(x => !IgnoredSheets.Contains(x.Name.Trim()))
            .ToList();
    }
}