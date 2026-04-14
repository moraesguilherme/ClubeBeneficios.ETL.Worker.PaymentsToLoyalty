using ClosedXML.Excel;

namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.FileReaders;

public interface IHotelWorkbookReader
{
    bool CanRead(string filePath);
    IXLWorksheet? GetWorksheet(XLWorkbook workbook);
}