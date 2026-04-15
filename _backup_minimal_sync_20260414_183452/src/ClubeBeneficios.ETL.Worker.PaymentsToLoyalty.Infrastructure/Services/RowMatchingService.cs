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
        _logger.LogInformation("RowMatchingService executado. Matching detalhado ficarÃ¡ na prÃ³xima etapa.");
        return Task.FromResult(0);
    }
}