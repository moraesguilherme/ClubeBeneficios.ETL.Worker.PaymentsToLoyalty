using System.Text;
using System.Text.Json;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Dtos;

namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.FileReaders;

public class CsvRawFileReader : IRawFileReader
{
    public bool CanRead(string filePath)
        => filePath.EndsWith(".csv", StringComparison.OrdinalIgnoreCase);

    public async Task<IReadOnlyList<ImportRawRowDto>> ReadAsync(string filePath, CancellationToken cancellationToken)
    {
        var lines = await File.ReadAllLinesAsync(filePath, Encoding.UTF8, cancellationToken);
        var result = new List<ImportRawRowDto>();

        for (var i = 0; i < lines.Length; i++)
        {
            var json = JsonSerializer.Serialize(new { raw = lines[i] ?? string.Empty });

            result.Add(new ImportRawRowDto
            {
                RowNumber = i + 1,
                ExternalRowKey = null,
                RawPayloadJson = json
            });
        }

        return result;
    }
}