using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Interfaces;
using Microsoft.Extensions.Logging;

namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Jobs;

public class LoyaltyGenerationJob
{
    private readonly ILoyaltyEventGenerationService _service;
    private readonly ILogger<LoyaltyGenerationJob> _logger;

    public LoyaltyGenerationJob(ILoyaltyEventGenerationService service, ILogger<LoyaltyGenerationJob> logger)
    {
        _service = service;
        _logger = logger;
    }

    public async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Iniciando LoyaltyGenerationJob.");
        await _service.GenerateEventsAsync(cancellationToken);
    }
}