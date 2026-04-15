using System.Security.Cryptography;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Interfaces;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Services;

public class FileImportService : IFileImportService
{
    private readonly ILogger<FileImportService> _logger;
    private readonly IEtlBatchRepository _batchRepository;
    private readonly IEtlRowRepository _rowRepository;
    private readonly EtlWorkerOptions _options;

    public FileImportService(
        ILogger<FileImportService> logger,
        IEtlBatchRepository batchRepository,
        IEtlRowRepository rowRepository,
        IOptions<EtlWorkerOptions> options)
    {
        _logger = logger;
        _batchRepository = batchRepository;
        _rowRepository = rowRepository;
        _options = options.Value;
    }

    public async Task<int> ImportPendingFilesAsync(CancellationToken cancellationToken)
    {
        if (!Directory.Exists(_options.WatchFolderPath))
        {
            _logger.LogWarning("Pasta monitorada nÃ£o encontrada: {Path}", _options.WatchFolderPath);
            return 0;
        }

        var files = Directory.GetFiles(_options.WatchFolderPath, "*.*", SearchOption.TopDirectoryOnly)
            .Where(f => f.EndsWith(".xlsx", StringComparison.OrdinalIgnoreCase) || f.EndsWith(".csv", StringComparison.OrdinalIgnoreCase))
            .OrderBy(f => f)
            .ToList();

        var imported = 0;

        foreach (var file in files)
        {
            var batchId = await ImportFileAsync(file, cancellationToken);
            if (batchId.HasValue)
            {
                imported++;
            }
        }

        return imported;
    }

    public async Task<Guid?> ImportFileAsync(string filePath, CancellationToken cancellationToken)
    {
        if (!File.Exists(filePath))
        {
            _logger.LogWarning("Arquivo nÃ£o encontrado: {FilePath}", filePath);
            return null;
        }

        var batchId = await _batchRepository.CreateBatchAsync(
            sourceName: "local_file",
            sourceType: "spreadsheet",
            fileName: Path.GetFileName(filePath),
            fileHash: await ComputeSha256Async(filePath, cancellationToken),
            createdByUserId: null,
            notes: null,
            cancellationToken: cancellationToken);

        var rowNumber = 0;
        var successRows = 0;
        var errorRows = 0;

        try
        {
            foreach (var line in await File.ReadAllLinesAsync(filePath, cancellationToken))
            {
                rowNumber++;

                try
                {
                    var payload = System.Text.Json.JsonSerializer.Serialize(new { raw = line });

                    await _rowRepository.CreateImportRowAsync(
                        batchId,
                        rowNumber,
                        null,
                        payload,
                        cancellationToken);

                    successRows++;
                }
                catch (Exception ex)
                {
                    errorRows++;
                    _logger.LogError(ex, "Erro ao gravar linha crua {RowNumber} do arquivo {FilePath}", rowNumber, filePath);
                }
            }

            await _batchRepository.SetBatchStatusAsync(
                batchId,
                errorRows > 0 ? "processed_with_errors" : "processed",
                successRows + errorRows,
                successRows + errorRows,
                successRows,
                errorRows,
                null,
                cancellationToken);

            return batchId;
        }
        catch (Exception ex)
        {
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