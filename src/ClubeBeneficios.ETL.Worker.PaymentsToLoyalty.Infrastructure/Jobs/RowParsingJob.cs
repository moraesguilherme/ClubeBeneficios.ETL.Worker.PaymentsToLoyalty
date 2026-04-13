using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Interfaces;
using Microsoft.Extensions.Logging;

namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Jobs;

public class RowParsingJob
{
    private readonly IRowParserService _service;
    private readonly ILogger<RowParsingJob> _logger;

    public RowParsingJob(IRowParserService service, ILogger<RowParsingJob> logger)
    {
        _service = service;
        _logger = logger;
    }

    public async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Iniciando RowParsingJob.");
        await _service.ParsePendingRowsAsync(cancellationToken);
    }
}