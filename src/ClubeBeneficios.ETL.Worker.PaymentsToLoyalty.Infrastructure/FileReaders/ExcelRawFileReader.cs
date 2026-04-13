using System.Text.Json;
using ClosedXML.Excel;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Dtos;

namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.FileReaders;

public class ExcelRawFileReader : IRawFileReader
{
    public bool CanRead(string filePath)
        => filePath.EndsWith(".xlsx", StringComparison.OrdinalIgnoreCase);

    public Task<IReadOnlyList<ImportRawRowDto>> ReadAsync(string filePath, CancellationToken cancellationToken)
    {
        var result = new List<ImportRawRowDto>();

        using var workbook = new XLWorkbook(filePath);
        var worksheet = workbook.Worksheets.First();

        var lastRow = worksheet.LastRowUsed()?.RowNumber() ?? 0;
        var lastColumn = worksheet.LastColumnUsed()?.ColumnNumber() ?? 0;

        for (var row = 1; row <= lastRow; row++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var cells = new Dictionary<string, string?>();
            for (var col = 1; col <= lastColumn; col++)
            {
                cells[$"col_{col}"] = worksheet.Cell(row, col).GetValue<string>();
            }

            var json = JsonSerializer.Serialize(cells);

            result.Add(new ImportRawRowDto
            {
                RowNumber = row,
                ExternalRowKey = null,
                RawPayloadJson = json
            });
        }

        return Task.FromResult<IReadOnlyList<ImportRawRowDto>>(result);
    }
}