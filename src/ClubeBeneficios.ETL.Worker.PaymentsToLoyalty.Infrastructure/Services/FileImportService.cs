using System.Security.Cryptography;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Interfaces;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Configuration;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.FileReaders;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Services;

public class FileImportService : IFileImportService
{
    private readonly ILogger<FileImportService> _logger;
    private readonly IEtlBatchRepository _batchRepository;
    private readonly IEtlRowRepository _rowRepository;
    private readonly EtlWorkerOptions _options;
    private readonly IReadOnlyCollection<IRawFileReader> _readers;

    public FileImportService(
        ILogger<FileImportService> logger,
        IEtlBatchRepository batchRepository,
        IEtlRowRepository rowRepository,
        IOptions<EtlWorkerOptions> options,
        IEnumerable<IRawFileReader> readers)
    {
        _logger = logger;
        _batchRepository = batchRepository;
        _rowRepository = rowRepository;
        _options = options.Value;
        _readers = readers.ToList();
    }

    public async Task<int> ImportPendingFilesAsync(CancellationToken cancellationToken)
    {
        if (!Directory.Exists(_options.WatchFolderPath))
        {
            _logger.LogWarning("Pasta monitorada nÃ£o encontrada: {Path}", _options.WatchFolderPath);
            return 0;
        }

        var files = Directory
            .GetFiles(_options.WatchFolderPath, "*.*", SearchOption.TopDirectoryOnly)
            .Where(f =>
                f.EndsWith(".csv", StringComparison.OrdinalIgnoreCase) ||
                f.EndsWith(".xlsx", StringComparison.OrdinalIgnoreCase))
            .OrderBy(f => f)
            .ToList();

        var importedCount = 0;

        foreach (var file in files)
        {
            var batchId = await ImportFileAsync(file, cancellationToken);
            if (batchId.HasValue)
            {
                importedCount++;
            }
        }

        return importedCount;
    }

    public async Task<Guid?> ImportFileAsync(string filePath, CancellationToken cancellationToken)
    {
        if (!File.Exists(filePath))
        {
            _logger.LogWarning("Arquivo nÃ£o encontrado: {FilePath}", filePath);
            return null;
        }

        var reader = _readers.FirstOrDefault(r => r.CanRead(filePath));
        if (reader is null)
        {
            _logger.LogWarning("Nenhum leitor disponÃ­vel para o arquivo: {FilePath}", filePath);
            return null;
        }

        var fileHash = await ComputeSha256Async(filePath, cancellationToken);

        var batchId = await _batchRepository.CreateBatchAsync(
            sourceName: "local_file",
            sourceType: "spreadsheet",
            fileName: Path.GetFileName(filePath),
            fileHash: fileHash,
            createdByUserId: null,
            notes: null,
            cancellationToken: cancellationToken);

        var successRows = 0;
        var errorRows = 0;

        try
        {
            var rows = await reader.ReadAsync(filePath, cancellationToken);

            foreach (var row in rows)
            {
                try
                {
                    await _rowRepository.CreateImportRowAsync(
                        batchId,
                        row.RowNumber,
                        row.ExternalRowKey,
                        row.RawPayloadJson,
                        cancellationToken);

                    successRows++;
                }
                catch (Exception ex)
                {
                    errorRows++;
                    _logger.LogError(ex, "Erro ao persistir a linha {RowNumber} do arquivo {FilePath}", row.RowNumber, filePath);
                }
            }

            var totalRows = successRows + errorRows;
            var status = errorRows > 0 ? "processed_with_errors" : "processed";

            await _batchRepository.SetBatchStatusAsync(
                batchId,
                status,
                totalRows,
                totalRows,
                successRows,
                errorRows,
                null,
                cancellationToken);

            _logger.LogInformation(
                "Arquivo importado. BatchId: {BatchId}, TotalRows: {TotalRows}, SuccessRows: {SuccessRows}, ErrorRows: {ErrorRows}",
                batchId,
                totalRows,
                successRows,
                errorRows);

            return batchId;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Falha geral ao importar o arquivo {FilePath}", filePath);

            await _batchRepository.SetBatchStatusAsync(
                batchId,
                "failed",
                successRows + errorRows,
                successRows + errorRows,
                successRows,
                errorRows,
                ex.Message,
                cancellationToken);

            throw;
        }
    }

    private static async Task<string> ComputeSha256Async(string filePath, CancellationToken cancellationToken)
    {
        await using var stream = File.OpenRead(filePath);
        using var sha = SHA256.Create();
        var hash = await sha.ComputeHashAsync(stream, cancellationToken);
        return Convert.ToHexString(hash);
    }
}