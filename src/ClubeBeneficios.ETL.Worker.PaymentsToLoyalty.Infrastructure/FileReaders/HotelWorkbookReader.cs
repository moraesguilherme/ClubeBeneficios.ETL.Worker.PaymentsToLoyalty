using ClosedXML.Excel;

namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.FileReaders;

public class HotelWorkbookReader : IHotelWorkbookReader
{
    public bool CanRead(string filePath)
    {
        return filePath.EndsWith(".xlsx", StringComparison.OrdinalIgnoreCase)
            && Path.GetFileName(filePath).Contains("HOTEL", StringComparison.OrdinalIgnoreCase);
    }

    public IXLWorksheet? GetWorksheet(XLWorkbook workbook)
    {
        return workbook.Worksheets
            .FirstOrDefault(x => string.Equals(x.Name.Trim(), "AGENDA 2026", StringComparison.OrdinalIgnoreCase));
    }
}