using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Interfaces;
using Microsoft.Extensions.Logging;

namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Jobs;

public class FileIngestionJob
{
    private readonly IFileImportService _service;
    private readonly ILogger<FileIngestionJob> _logger;

    public FileIngestionJob(IFileImportService service, ILogger<FileIngestionJob> logger)
    {
        _service = service;
        _logger = logger;
    }

    public async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Iniciando FileIngestionJob.");
        await _service.ImportPendingFilesAsync(cancellationToken);
    }
}