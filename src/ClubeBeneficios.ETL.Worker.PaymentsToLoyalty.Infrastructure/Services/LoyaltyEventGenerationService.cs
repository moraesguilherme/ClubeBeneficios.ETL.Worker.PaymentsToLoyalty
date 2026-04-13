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
        _logger.LogInformation("LoyaltyEventGenerationService executado.");
        return Task.FromResult(0);
    }

    public Task RunMaintenanceAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Rotina de manutenÃ§Ã£o de loyalty executada.");
        return Task.CompletedTask;
    }
}