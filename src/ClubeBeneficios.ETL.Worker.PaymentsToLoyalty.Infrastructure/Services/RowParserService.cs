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
        _logger.LogInformation("RowParserService desativado neste escopo mÃ­nimo da ETL.");
        return Task.FromResult(0);
    }
}