using System.Globalization;
using System.Security.Cryptography;
using System.Text.Json;
using ClosedXML.Excel;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Dtos;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Interfaces;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Services;

public class FileImportService : IFileImportService
{
    private static readonly HashSet<string> IgnoredCrecheSheets = new(StringComparer.OrdinalIgnoreCase)
    {
        "LISTA APOIO",
        "BRINDES",
        "PLANOS HOTEL",
        "TURMA"
    };

    private readonly ILogger<FileImportService> _logger;
    private readonly IEtlBatchRepository _batchRepository;
    private readonly IEtlRowRepository _rowRepository;
    private readonly IEtlProcessingRunRepository _runRepository;
    private readonly EtlWorkerOptions _options;

    public FileImportService(
        ILogger<FileImportService> logger,
        IEtlBatchRepository batchRepository,
        IEtlRowRepository rowRepository,
        IEtlProcessingRunRepository runRepository,
        IOptions<EtlWorkerOptions> options)
    {
        _logger = logger;
        _batchRepository = batchRepository;
        _rowRepository = rowRepository;
        _runRepository = runRepository;
        _options = options.Value;
    }

    public async Task<int> ImportPendingFilesAsync(CancellationToken cancellationToken)
    {
        if (!Directory.Exists(_options.WatchFolderPath))
        {
            _logger.LogWarning("Pasta monitorada não encontrada: {Path}", _options.WatchFolderPath);
            return 0;
        }

        var files = Directory.GetFiles(_options.WatchFolderPath, "*.*", SearchOption.TopDirectoryOnly)
            .Where(f => f.EndsWith(".xlsx", StringComparison.OrdinalIgnoreCase) ||
                        f.EndsWith(".csv", StringComparison.OrdinalIgnoreCase))
            .OrderBy(f => f)
            .ToList();

        var imported = 0;

        foreach (var file in files)
        {
            var batchId = await _batchRepository.CreateBatchAsync(
                sourceName: "local_file",
                sourceType: "spreadsheet",
                fileName: Path.GetFileName(file),
                fileHash: await ComputeSha256Async(file, cancellationToken),
                createdByUserId: null,
                notes: null,
                cancellationToken: cancellationToken);

            var runId = await _runRepository.CreateRunAsync(
                batchId,
                "file_ingestion",
                "running",
                cancellationToken);

            var successRows = 0;
            var errorRows = 0;

            try
            {
                if (file.EndsWith(".xlsx", StringComparison.OrdinalIgnoreCase))
                {
                    successRows = await ImportExcelAsync(file, batchId, cancellationToken);
                }
                else
                {
                    successRows = await ImportCsvAsync(file, batchId, cancellationToken);
                }

                await _batchRepository.SetBatchStatusAsync(
                    batchId,
                    "processed",
                    DateTime.UtcNow,
                    null,
                    cancellationToken);

                await _runRepository.FinishRunAsync(
                    runId,
                    "processed",
                    successRows + errorRows,
                    successRows,
                    errorRows,
                    null,
                    cancellationToken);

                imported++;
            }
            catch (Exception ex)
            {
                errorRows++;

                await _batchRepository.SetBatchStatusAsync(
                    batchId,
                    "failed",
                    DateTime.UtcNow,
                    ex.Message,
                    cancellationToken);

                await _runRepository.FinishRunAsync(
                    runId,
                    "failed",
                    successRows + errorRows,
                    successRows,
                    errorRows,
                    ex.ToString(),
                    cancellationToken);

                _logger.LogError(ex, "Erro ao importar arquivo {File}", file);
            }
        }

        return imported;
    }

    private async Task<int> ImportCsvAsync(string filePath, Guid batchId, CancellationToken cancellationToken)
    {
        var lines = await File.ReadAllLinesAsync(filePath, cancellationToken);
        var rowNumber = 0;
        var count = 0;

        foreach (var line in lines)
        {
            rowNumber++;

            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            var dto = new ImportRowCreateDto
            {
                BatchId = batchId,
                RowNumber = rowNumber,
                RawPayloadJson = JsonSerializer.Serialize(new { raw = line }),
                SourceFileType = "csv_generic"
            };

            await _rowRepository.CreateImportRowAsync(dto, cancellationToken);
            count++;
        }

        return count;
    }

    private async Task<int> ImportExcelAsync(string filePath, Guid batchId, CancellationToken cancellationToken)
    {
        using var workbook = new XLWorkbook(filePath);

        if (Path.GetFileName(filePath).Contains("HOTEL", StringComparison.OrdinalIgnoreCase))
        {
            return await ImportHotelAsync(workbook, batchId, cancellationToken);
        }

        if (Path.GetFileName(filePath).Contains("CRECHE", StringComparison.OrdinalIgnoreCase))
        {
            return await ImportCrecheAsync(workbook, batchId, cancellationToken);
        }

        throw new InvalidOperationException($"Arquivo XLSX não reconhecido para ETL: {filePath}");
    }

    private async Task<int> ImportHotelAsync(XLWorkbook workbook, Guid batchId, CancellationToken cancellationToken)
    {
        var sheet = workbook.Worksheets.FirstOrDefault(x =>
            string.Equals(x.Name.Trim(), "AGENDA 2026", StringComparison.OrdinalIgnoreCase));

        if (sheet is null)
        {
            throw new InvalidOperationException("Aba 'AGENDA 2026' não encontrada na planilha de hotel.");
        }

        const int headerRow = 2;
        var lastRow = sheet.LastRowUsed()?.RowNumber() ?? 0;
        var lastCol = sheet.LastColumnUsed()?.ColumnNumber() ?? 0;

        var headers = ReadHeaders(sheet, headerRow, lastCol);
        var count = 0;

        for (var row = headerRow + 1; row <= lastRow; row++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var data = ReadRow(sheet, headers, row);
            if (IsEmptyDataRow(data))
            {
                continue;
            }

            var dto = new ImportRowCreateDto
            {
                BatchId = batchId,
                RowNumber = row,
                RawPayloadJson = JsonSerializer.Serialize(data),
                SourceFileType = "hotel_agenda",
                SourceSheetName = sheet.Name,
                SourceSheetGroup = "hotel",
                ReferenceYear = 2026,

                CustomerNameRaw = GetValue(data, "TUTOR"),
                CustomerDocumentRaw = GetValue(data, "CPF"),
                CustomerPhoneRaw = GetValue(data, "TELEFONE"),
                PetNameRaw = GetValue(data, "CACHORRO"),

                ServiceTypeRaw = "hotel",
                LodgingTypeRaw = GetValue(data, "PERÍODO"),

                GrossAmount = ParseDecimal(GetValue(data, "VALOR")),
                NetAmount = ParseDecimal(GetValue(data, "VALOR")),

                StartDate = ParseDate(GetValue(data, "INICIAL")),
                EndDate = ParseDate(GetValue(data, "FINAL")),

                PaymentMethodRaw = GetValue(data, "PGTO"),
                PaymentMethodNormalized = NormalizePaymentMethod(GetValue(data, "PGTO")),
                PaymentStatusRaw = GetValue(data, "PGTO")
            };

            dto.OccurredAt = dto.StartDate;
            dto.CompetenceDate = dto.StartDate;

            await _rowRepository.CreateImportRowAsync(dto, cancellationToken);
            count++;
        }

        return count;
    }

    private async Task<int> ImportCrecheAsync(XLWorkbook workbook, Guid batchId, CancellationToken cancellationToken)
    {
        var sheets = workbook.Worksheets
            .Where(x => !IgnoredCrecheSheets.Contains(x.Name.Trim()))
            .ToList();

        var count = 0;

        foreach (var sheet in sheets)
        {
            var headerRow = FindHeaderRow(sheet);
            if (headerRow is null)
            {
                _logger.LogWarning("Cabeçalho não encontrado na aba {Sheet}", sheet.Name);
                continue;
            }

            var lastRow = sheet.LastRowUsed()?.RowNumber() ?? 0;
            var lastCol = sheet.LastColumnUsed()?.ColumnNumber() ?? 0;
            var headers = ReadHeaders(sheet, headerRow.Value, lastCol);

            var month = ResolveMonth(sheet.Name);

            for (var row = headerRow.Value + 1; row <= lastRow; row++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var data = ReadRow(sheet, headers, row);
                if (IsEmptyDataRow(data))
                {
                    continue;
                }

                var dto = new ImportRowCreateDto
                {
                    BatchId = batchId,
                    RowNumber = row,
                    RawPayloadJson = JsonSerializer.Serialize(data),
                    SourceFileType = "creche_mensal",
                    SourceSheetName = sheet.Name,
                    SourceSheetGroup = "creche",
                    ReferenceYear = 2026,
                    ReferenceMonth = month,

                    CustomerNameRaw = GetFirstValue(data, new[] { "TUTOR", "DONO" }),
                    CustomerDocumentRaw = GetValue(data, "CPF"),
                    CustomerPhoneRaw = GetValue(data, "TELEFONE"),
                    PetNameRaw = GetValue(data, "CACHORRO"),

                    ServiceTypeRaw = GetValue(data, "TIPO") ?? "creche",

                    GrossAmount = ParseDecimal(GetValue(data, "VALOR")),
                    NetAmount = ParseDecimal(GetFirstValue(data, new[] { "SOMA TOTAL", "VALOR" })),
                    TaxiAmount = ParseDecimal(GetValue(data, "TÁXI")),
                    Quantity = 1m,

                    PaymentMethodRaw = GetValue(data, "FORMA DE PAGAMENTO"),
                    PaymentMethodNormalized = NormalizePaymentMethod(GetValue(data, "FORMA DE PAGAMENTO")),
                    PaymentStatusRaw = GetValue(data, "STATUS"),

                    CompetenceDate = month is null ? null : new DateTime(2026, month.Value, 1),
                    DescriptionRaw = GetValue(data, "DESCRIÇÃO"),
                    ObservationRaw = GetFirstValue(data, new[] { "OBSERVAÇÃO", "OBS" })
                };

                await _rowRepository.CreateImportRowAsync(dto, cancellationToken);
                count++;
            }
        }

        return count;
    }

    private static Dictionary<int, string> ReadHeaders(IXLWorksheet sheet, int headerRow, int lastCol)
    {
        var headers = new Dictionary<int, string>();

        for (var col = 1; col <= lastCol; col++)
        {
            var raw = sheet.Cell(headerRow, col).GetValue<string>()?.Trim();
            if (!string.IsNullOrWhiteSpace(raw))
            {
                headers[col] = NormalizeHeader(raw);
            }
        }

        return headers;
    }

    private static Dictionary<string, string?> ReadRow(IXLWorksheet sheet, Dictionary<int, string> headers, int row)
    {
        var result = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);

        foreach (var kvp in headers)
        {
            result[kvp.Value] = sheet.Cell(row, kvp.Key).GetValue<string>()?.Trim();
        }

        return result;
    }

    private static bool IsEmptyDataRow(Dictionary<string, string?> data)
    {
        return data.Values.All(v => string.IsNullOrWhiteSpace(v));
    }

    private static int? FindHeaderRow(IXLWorksheet sheet)
    {
        var lastRow = Math.Min(sheet.LastRowUsed()?.RowNumber() ?? 0, 20);
        var lastCol = Math.Min(sheet.LastColumnUsed()?.ColumnNumber() ?? 0, 20);

        for (var row = 1; row <= lastRow; row++)
        {
            var values = Enumerable.Range(1, lastCol)
                .Select(col => NormalizeHeader(sheet.Cell(row, col).GetValue<string>() ?? string.Empty))
                .ToList();

            if (values.Contains("STATUS") &&
                values.Contains("CACHORRO") &&
                (values.Contains("TUTOR") || values.Contains("DONO")))
            {
                return row;
            }
        }

        return null;
    }

    private static string? GetValue(Dictionary<string, string?> data, string key)
    {
        data.TryGetValue(NormalizeHeader(key), out var value);
        return value;
    }

    private static string? GetFirstValue(Dictionary<string, string?> data, IEnumerable<string> keys)
    {
        foreach (var key in keys)
        {
            var value = GetValue(data, key);
            if (!string.IsNullOrWhiteSpace(value))
            {
                return value;
            }
        }

        return null;
    }

    private static string NormalizeHeader(string value)
    {
        return value
            .Trim()
            .ToUpperInvariant()
            .Replace("Á", "A")
            .Replace("À", "A")
            .Replace("Â", "A")
            .Replace("Ã", "A")
            .Replace("É", "E")
            .Replace("Ê", "E")
            .Replace("Í", "I")
            .Replace("Ó", "O")
            .Replace("Ô", "O")
            .Replace("Õ", "O")
            .Replace("Ú", "U")
            .Replace("Ç", "C");
    }

    private static decimal? ParseDecimal(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        value = value.Replace("R$", string.Empty).Trim();

        if (decimal.TryParse(value, NumberStyles.Any, new CultureInfo("pt-BR"), out var result))
            return result;

        return null;
    }

    private static DateTime? ParseDate(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        if (DateTime.TryParse(value, new CultureInfo("pt-BR"), DateTimeStyles.None, out var result))
            return result;

        return null;
    }

    private static string? NormalizePaymentMethod(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        var normalized = value.Trim().ToLowerInvariant();

        if (normalized.Contains("pix")) return "pix";
        if (normalized.Contains("dinheiro") || normalized.Contains("especie") || normalized.Contains("espécie")) return "cash";
        if (normalized.Contains("credito") || normalized.Contains("crédito")) return "credit_card";
        if (normalized.Contains("debito") || normalized.Contains("débito")) return "debit_card";
        if (normalized.Contains("boleto")) return "boleto";
        if (normalized.Contains("transfer")) return "bank_transfer";

        return "other";
    }

    private static int? ResolveMonth(string sheetName)
    {
        return NormalizeHeader(sheetName) switch
        {
            "JANEIRO" => 1,
            "FEVEREIRO" => 2,
            "MARCO" => 3,
            "ABRIL" => 4,
            "MAIO" => 5,
            "JUNHO" => 6,
            "JULHO" => 7,
            "AGOSTO" => 8,
            "SETEMBRO" => 9,
            "OUTUBRO" => 10,
            "NOVEMBRO" => 11,
            "DEZEMBRO" => 12,
            _ => null
        };
    }

    private static async Task<string> ComputeSha256Async(string filePath, CancellationToken cancellationToken)
    {
        await using var stream = File.OpenRead(filePath);
        using var sha = SHA256.Create();
        var hash = await sha.ComputeHashAsync(stream, cancellationToken);
        return Convert.ToHexString(hash);
    }
}