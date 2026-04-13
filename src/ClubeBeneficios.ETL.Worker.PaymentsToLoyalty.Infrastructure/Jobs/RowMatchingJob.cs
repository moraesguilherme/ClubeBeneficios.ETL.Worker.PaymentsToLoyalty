using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Interfaces;
using Microsoft.Extensions.Logging;

namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Jobs;

public class RowMatchingJob
{
    private readonly IRowMatchingService _service;
    private readonly ILogger<RowMatchingJob> _logger;

    public RowMatchingJob(IRowMatchingService service, ILogger<RowMatchingJob> logger)
    {
        _service = service;
        _logger = logger;
    }

    public async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Iniciando RowMatchingJob.");
        await _service.MatchParsedRowsAsync(cancellationToken);
    }
}