using ClosedXML.Excel;

namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.FileReaders;

public interface ICrecheWorkbookReader
{
    bool CanRead(string filePath);
    IReadOnlyList<IXLWorksheet> GetMonthlyWorksheets(XLWorkbook workbook);
}