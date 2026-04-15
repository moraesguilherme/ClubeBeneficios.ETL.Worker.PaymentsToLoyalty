param(
    [string]$BasePath = "C:\ClubeBeneficios.ETL.Worker.PaymentsToLoyalty",
    [switch]$CreateBackup = $true
)

$ErrorActionPreference = "Stop"

function Write-Step {
    param([string]$Message)
    Write-Host ""
    Write-Host "==> $Message" -ForegroundColor Cyan
}

function Ensure-Directory {
    param([string]$Path)
    if (-not (Test-Path $Path)) {
        New-Item -ItemType Directory -Path $Path -Force | Out-Null
    }
}

function Write-Utf8File {
    param(
        [string]$Path,
        [string]$Content
    )

    $directory = Split-Path -Parent $Path
    Ensure-Directory $directory

    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($Path, $Content, $utf8NoBom)
}

function Backup-FileIfExists {
    param(
        [string]$Path,
        [string]$BackupRoot
    )

    if (-not (Test-Path $Path)) {
        return
    }

    $relative = $Path.Substring($BasePath.Length).TrimStart('\')
    $backupPath = Join-Path $BackupRoot $relative
    $backupDir = Split-Path -Parent $backupPath
    Ensure-Directory $backupDir
    Copy-Item $Path $backupPath -Force
}

if (-not (Test-Path $BasePath)) {
    throw "BasePath não encontrado: $BasePath"
}

$srcPath = Join-Path $BasePath "src"

$rootProject = "ClubeBeneficios.ETL.Worker.PaymentsToLoyalty"
$appProject = "$rootProject.Application"
$infraProject = "$rootProject.Infrastructure"
$domainProject = "$rootProject.Domain"

$workerPath = Join-Path $srcPath $rootProject
$appPath = Join-Path $srcPath $appProject
$infraPath = Join-Path $srcPath $infraProject
$domainPath = Join-Path $srcPath $domainProject

$solutionFile = Get-ChildItem -Path $BasePath -Filter *.sln -File | Select-Object -First 1
if (-not $solutionFile) {
    throw "Nenhuma solution (.sln) encontrada em $BasePath"
}

$backupRoot = Join-Path $BasePath "_backup_minimal_sync_$(Get-Date -Format 'yyyyMMdd_HHmmss')"

Write-Step "Validando estrutura"
@($workerPath, $appPath, $infraPath, $domainPath) | ForEach-Object {
    if (-not (Test-Path $_)) {
        throw "Estrutura esperada não encontrada: $_"
    }
}

if ($CreateBackup) {
    Write-Step "Criando backup"
    $filesToBackup = @(
        (Join-Path $workerPath "Program.cs"),
        (Join-Path $workerPath "appsettings.json"),
        (Join-Path $workerPath "$rootProject.csproj"),
        (Join-Path $workerPath "HostedServices\PipelineHostedService.cs"),

        (Join-Path $appPath "Interfaces\IFileImportService.cs"),
        (Join-Path $appPath "Interfaces\IEtlBatchRepository.cs"),
        (Join-Path $appPath "Interfaces\IEtlRowRepository.cs"),
        (Join-Path $appPath "Interfaces\IEtlProcessingRunRepository.cs"),
        (Join-Path $appPath "Interfaces\IRowParserService.cs"),
        (Join-Path $appPath "Interfaces\IRowMatchingService.cs"),
        (Join-Path $appPath "Interfaces\ILoyaltyEventGenerationService.cs"),

        (Join-Path $infraPath "Repositories\EtlBatchRepository.cs"),
        (Join-Path $infraPath "Repositories\EtlRowRepository.cs"),
        (Join-Path $infraPath "Services\FileImportService.cs"),
        (Join-Path $infraPath "Services\RowParserService.cs"),
        (Join-Path $infraPath "Services\RowMatchingService.cs"),
        (Join-Path $infraPath "Services\LoyaltyEventGenerationService.cs")
    )

    foreach ($file in $filesToBackup) {
        Backup-FileIfExists -Path $file -BackupRoot $backupRoot
    }
}

Write-Step "Garantindo diretórios"
@(
    (Join-Path $appPath "Dtos"),
    (Join-Path $appPath "Interfaces"),
    (Join-Path $infraPath "Repositories"),
    (Join-Path $infraPath "Services")
) | ForEach-Object { Ensure-Directory $_ }

Write-Step "Escrevendo DTO mínimo de importação"

Write-Utf8File -Path (Join-Path $appPath "Dtos\ImportRowCreateDto.cs") -Content @'
namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Dtos;

public class ImportRowCreateDto
{
    public Guid BatchId { get; set; }
    public int RowNumber { get; set; }
    public string? ExternalRowKey { get; set; }
    public string RawPayloadJson { get; set; } = string.Empty;
    public DateTime? OccurredAt { get; set; }
    public DateTime? CompetenceDate { get; set; }
    public string? CustomerNameRaw { get; set; }
    public string? CustomerDocumentRaw { get; set; }
    public string? CustomerEmailRaw { get; set; }
    public string? CustomerPhoneRaw { get; set; }
    public string? PetNameRaw { get; set; }
    public string? PartnerNameRaw { get; set; }
    public string? ServiceTypeRaw { get; set; }
    public string? PlanNameRaw { get; set; }
    public string? PackageNameRaw { get; set; }
    public string? LodgingTypeRaw { get; set; }
    public string? PaymentMethodRaw { get; set; }
    public string? PaymentMethodNormalized { get; set; }
    public decimal? GrossAmount { get; set; }
    public decimal? DiscountAmount { get; set; }
    public decimal? NetAmount { get; set; }
    public decimal? Quantity { get; set; }
}
'@

Write-Step "Escrevendo interfaces mínimas"

Write-Utf8File -Path (Join-Path $appPath "Interfaces\IFileImportService.cs") -Content @'
namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Interfaces;

public interface IFileImportService
{
    Task<int> ImportPendingFilesAsync(CancellationToken cancellationToken);
}
'@

Write-Utf8File -Path (Join-Path $appPath "Interfaces\IEtlBatchRepository.cs") -Content @'
namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Interfaces;

public interface IEtlBatchRepository
{
    Task<Guid> CreateBatchAsync(
        string sourceName,
        string sourceType,
        string? fileName,
        string? fileHash,
        Guid? createdByUserId,
        string? notes,
        CancellationToken cancellationToken);

    Task SetBatchStatusAsync(
        Guid batchId,
        string status,
        DateTime? finishedAt,
        string? notes,
        CancellationToken cancellationToken);
}
'@

Write-Utf8File -Path (Join-Path $appPath "Interfaces\IEtlRowRepository.cs") -Content @'
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Dtos;

namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Interfaces;

public interface IEtlRowRepository
{
    Task<long> CreateImportRowAsync(
        ImportRowCreateDto dto,
        CancellationToken cancellationToken);
}
'@

Write-Utf8File -Path (Join-Path $appPath "Interfaces\IEtlProcessingRunRepository.cs") -Content @'
namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Interfaces;

public interface IEtlProcessingRunRepository
{
    Task<Guid> CreateRunAsync(
        Guid? batchId,
        string runType,
        string status,
        CancellationToken cancellationToken);

    Task FinishRunAsync(
        Guid id,
        string status,
        int processedItems,
        int successItems,
        int errorItems,
        string? logSummary,
        CancellationToken cancellationToken);
}
'@

Write-Utf8File -Path (Join-Path $appPath "Interfaces\IRowParserService.cs") -Content @'
namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Interfaces;

public interface IRowParserService
{
    Task<int> ParsePendingRowsAsync(CancellationToken cancellationToken);
}
'@

Write-Utf8File -Path (Join-Path $appPath "Interfaces\IRowMatchingService.cs") -Content @'
namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Interfaces;

public interface IRowMatchingService
{
    Task<int> MatchParsedRowsAsync(CancellationToken cancellationToken);
}
'@

Write-Utf8File -Path (Join-Path $appPath "Interfaces\ILoyaltyEventGenerationService.cs") -Content @'
namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Interfaces;

public interface ILoyaltyEventGenerationService
{
    Task<int> GenerateEventsAsync(CancellationToken cancellationToken);
    Task RunMaintenanceAsync(CancellationToken cancellationToken);
}
'@

Write-Step "Regravando EtlBatchRepository"

Write-Utf8File -Path (Join-Path $infraPath "Repositories\EtlBatchRepository.cs") -Content @'
using System.Data;
using Dapper;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Interfaces;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Persistence;

namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Repositories;

public class EtlBatchRepository : RepositoryBase, IEtlBatchRepository
{
    public EtlBatchRepository(IDbConnectionFactory connectionFactory) : base(connectionFactory)
    {
    }

    public async Task<Guid> CreateBatchAsync(
        string sourceName,
        string sourceType,
        string? fileName,
        string? fileHash,
        Guid? createdByUserId,
        string? notes,
        CancellationToken cancellationToken)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);

        var id = Guid.NewGuid();
        var startedAt = DateTime.UtcNow;

        var parameters = new DynamicParameters();
        parameters.Add("@Id", id);
        parameters.Add("@SourceName", sourceName);
        parameters.Add("@SourceType", sourceType);
        parameters.Add("@FileName", fileName);
        parameters.Add("@FileHash", fileHash);
        parameters.Add("@StartedAt", startedAt);
        parameters.Add("@CreatedByUserId", createdByUserId);
        parameters.Add("@Notes", notes);

        await connection.ExecuteAsync(
            new CommandDefinition(
                "dbo.usp_etl_import_batch_create",
                parameters,
                commandType: CommandType.StoredProcedure,
                cancellationToken: cancellationToken));

        return id;
    }

    public async Task SetBatchStatusAsync(
        Guid batchId,
        string status,
        DateTime? finishedAt,
        string? notes,
        CancellationToken cancellationToken)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);

        var parameters = new DynamicParameters();
        parameters.Add("@BatchId", batchId);
        parameters.Add("@Status", status);
        parameters.Add("@FinishedAt", finishedAt);
        parameters.Add("@Notes", notes);

        await connection.ExecuteAsync(
            new CommandDefinition(
                "dbo.usp_etl_import_batch_set_status",
                parameters,
                commandType: CommandType.StoredProcedure,
                cancellationToken: cancellationToken));
    }
}
'@

Write-Step "Regravando EtlRowRepository"

Write-Utf8File -Path (Join-Path $infraPath "Repositories\EtlRowRepository.cs") -Content @'
using System.Data;
using Dapper;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Dtos;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Interfaces;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Persistence;

namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Repositories;

public class EtlRowRepository : RepositoryBase, IEtlRowRepository
{
    public EtlRowRepository(IDbConnectionFactory connectionFactory) : base(connectionFactory)
    {
    }

    public async Task<long> CreateImportRowAsync(
        ImportRowCreateDto dto,
        CancellationToken cancellationToken)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);

        var parameters = new DynamicParameters();
        parameters.Add("@BatchId", dto.BatchId);
        parameters.Add("@RowNumber", dto.RowNumber);
        parameters.Add("@ExternalRowKey", dto.ExternalRowKey);
        parameters.Add("@RawPayloadJson", dto.RawPayloadJson);
        parameters.Add("@OccurredAt", dto.OccurredAt);
        parameters.Add("@CompetenceDate", dto.CompetenceDate);
        parameters.Add("@CustomerNameRaw", dto.CustomerNameRaw);
        parameters.Add("@CustomerDocumentRaw", dto.CustomerDocumentRaw);
        parameters.Add("@CustomerEmailRaw", dto.CustomerEmailRaw);
        parameters.Add("@CustomerPhoneRaw", dto.CustomerPhoneRaw);
        parameters.Add("@PetNameRaw", dto.PetNameRaw);
        parameters.Add("@PartnerNameRaw", dto.PartnerNameRaw);
        parameters.Add("@ServiceTypeRaw", dto.ServiceTypeRaw);
        parameters.Add("@PlanNameRaw", dto.PlanNameRaw);
        parameters.Add("@PackageNameRaw", dto.PackageNameRaw);
        parameters.Add("@LodgingTypeRaw", dto.LodgingTypeRaw);
        parameters.Add("@PaymentMethodRaw", dto.PaymentMethodRaw);
        parameters.Add("@PaymentMethodNormalized", dto.PaymentMethodNormalized);
        parameters.Add("@GrossAmount", dto.GrossAmount);
        parameters.Add("@DiscountAmount", dto.DiscountAmount);
        parameters.Add("@NetAmount", dto.NetAmount);
        parameters.Add("@Quantity", dto.Quantity);

        var row = await connection.QuerySingleAsync(
            new CommandDefinition(
                "dbo.usp_etl_import_row_create",
                parameters,
                commandType: CommandType.StoredProcedure,
                cancellationToken: cancellationToken));

        return (long)row.id;
    }
}
'@

Write-Step "Regravando FileImportService com parser real de hotel/creche"

Write-Utf8File -Path (Join-Path $infraPath "Services\FileImportService.cs") -Content @'
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
            .Where(f => f.EndsWith(".xlsx", StringComparison.OrdinalIgnoreCase) || f.EndsWith(".csv", StringComparison.OrdinalIgnoreCase))
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
                RawPayloadJson = JsonSerializer.Serialize(new { raw = line })
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

        var headerRow = 2;
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
                PaymentMethodNormalized = NormalizePaymentMethod(GetValue(data, "PGTO"))
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

            for (var row = headerRow.Value + 1; row <= lastRow; row++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var data = ReadRow(sheet, headers, row);
                if (IsEmptyDataRow(data))
                {
                    continue;
                }

                var month = ResolveMonth(sheet.Name);

                var dto = new ImportRowCreateDto
                {
                    BatchId = batchId,
                    RowNumber = row,
                    RawPayloadJson = JsonSerializer.Serialize(data),
                    CustomerNameRaw = GetFirstValue(data, @("TUTOR", "DONO")),
                    CustomerDocumentRaw = GetValue(data, "CPF"),
                    CustomerPhoneRaw = GetValue(data, "TELEFONE"),
                    PetNameRaw = GetValue(data, "CACHORRO"),
                    ServiceTypeRaw = GetValue(data, "TIPO") ?? "creche",
                    GrossAmount = ParseDecimal(GetValue(data, "VALOR")),
                    NetAmount = ParseDecimal(GetFirstValue(data, @("SOMA TOTAL", "VALOR"))),
                    DiscountAmount = null,
                    PaymentMethodRaw = GetValue(data, "FORMA DE PAGAMENTO"),
                    PaymentMethodNormalized = NormalizePaymentMethod(GetValue(data, "FORMA DE PAGAMENTO")),
                    Quantity = 1m,
                    CompetenceDate = month is null ? null : new DateTime(2026, month.Value, 1),
                    DescriptionRaw = GetValue(data, "DESCRIÇÃO"),
                    ObservationRaw = GetFirstValue(data, @("OBSERVAÇÃO", "OBS"))
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

        for (var row = 1; row <= lastRow; row++)
        {
            var values = Enumerable.Range(1, Math.Min(sheet.LastColumnUsed()?.ColumnNumber() ?? 0, 20))
                .Select(col => NormalizeHeader(sheet.Cell(row, col).GetValue<string>() ?? string.Empty))
                .ToList();

            if (values.Contains("STATUS") && values.Contains("CACHORRO"))
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
'@

Write-Step "Transformando serviços fora de escopo em no-op"

Write-Utf8File -Path (Join-Path $infraPath "Services\RowParserService.cs") -Content @'
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Interfaces;
using Microsoft.Extensions.Logging;

namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Services;

public class RowParserService : IRowParserService
{
    private readonly ILogger<RowParserService> _logger;

    public RowParserService(ILogger<RowParserService> logger)
    {
        _logger = logger;
    }

    public Task<int> ParsePendingRowsAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("RowParserService desativado neste escopo mínimo da ETL.");
        return Task.FromResult(0);
    }
}
'@

Write-Utf8File -Path (Join-Path $infraPath "Services\RowMatchingService.cs") -Content @'
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Interfaces;
using Microsoft.Extensions.Logging;

namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Services;

public class RowMatchingService : IRowMatchingService
{
    private readonly ILogger<RowMatchingService> _logger;

    public RowMatchingService(ILogger<RowMatchingService> logger)
    {
        _logger = logger;
    }

    public Task<int> MatchParsedRowsAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("RowMatchingService fora do escopo atual da ETL.");
        return Task.FromResult(0);
    }
}
'@

Write-Utf8File -Path (Join-Path $infraPath "Services\LoyaltyEventGenerationService.cs") -Content @'
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Interfaces;
using Microsoft.Extensions.Logging;

namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Services;

public class LoyaltyEventGenerationService : ILoyaltyEventGenerationService
{
    private readonly ILogger<LoyaltyEventGenerationService> _logger;

    public LoyaltyEventGenerationService(ILogger<LoyaltyEventGenerationService> logger)
    {
        _logger = logger;
    }

    public Task<int> GenerateEventsAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("LoyaltyEventGenerationService fora do escopo atual da ETL.");
        return Task.FromResult(0);
    }

    public Task RunMaintenanceAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Maintenance fora do escopo atual da ETL.");
        return Task.CompletedTask;
    }
}
'@

Write-Step "Regravando Program.cs"

Write-Utf8File -Path (Join-Path $workerPath "Program.cs") -Content @'
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Interfaces;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Configuration;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Jobs;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Persistence;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Repositories;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Services;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.HostedServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<EtlWorkerOptions>(
    builder.Configuration.GetSection("EtlWorker"));

builder.Services.AddSingleton<IDbConnectionFactory, SqlConnectionFactory>();

builder.Services.AddScoped<IEtlBatchRepository, EtlBatchRepository>();
builder.Services.AddScoped<IEtlRowRepository, EtlRowRepository>();
builder.Services.AddScoped<IEtlProcessingRunRepository, EtlProcessingRunRepository>();

builder.Services.AddScoped<IFileImportService, FileImportService>();
builder.Services.AddScoped<IRowParserService, RowParserService>();
builder.Services.AddScoped<IRowMatchingService, RowMatchingService>();
builder.Services.AddScoped<ILoyaltyEventGenerationService, LoyaltyEventGenerationService>();

builder.Services.AddScoped<FileIngestionJob>();
builder.Services.AddScoped<RowParsingJob>();
builder.Services.AddScoped<RowMatchingJob>();
builder.Services.AddScoped<LoyaltyGenerationJob>();
builder.Services.AddScoped<LoyaltyMaintenanceJob>();

builder.Services.AddHostedService<PipelineHostedService>();

var host = builder.Build();
await host.RunAsync();
'@

Write-Step "Atualizando appsettings.json"

Write-Utf8File -Path (Join-Path $workerPath "appsettings.json") -Content @'
{
  "ConnectionStrings": {
    "DefaultConnection": "Server=localhost;Database=ClubeBeneficiosDb;Trusted_Connection=True;TrustServerCertificate=True;"
  },
  "EtlWorker": {
    "Mode": "watch",
    "WatchFolderPath": "C:\\ETL\\inbound",
    "ProcessedFolderPath": "C:\\ETL\\processed",
    "ErrorFolderPath": "C:\\ETL\\error",
    "FilePath": null,
    "PollingIntervalSeconds": 30,
    "TopRowsPerCycle": 100,
    "EnableParsingJob": false,
    "EnableMatchingJob": false,
    "EnableLoyaltyGenerationJob": false,
    "EnableMaintenanceJob": false
  }
}
'@

Write-Step "Limpando bin/obj"
Get-ChildItem -Path $srcPath -Directory -Recurse -Force |
    Where-Object { $_.Name -in @("bin", "obj") } |
    ForEach-Object {
        Remove-Item $_.FullName -Recurse -Force -ErrorAction SilentlyContinue
    }

Write-Step "Restaurando e compilando"
dotnet restore $solutionFile.FullName
dotnet build $solutionFile.FullName

Write-Step "Concluído"
Write-Host "ETL simplificada e alinhada ao escopo mínimo." -ForegroundColor Green
if ($CreateBackup) {
    Write-Host "Backup em: $backupRoot" -ForegroundColor Yellow
}