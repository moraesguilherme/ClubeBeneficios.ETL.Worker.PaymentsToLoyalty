using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Interfaces;
using Microsoft.Extensions.Logging;

namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Jobs;

public class LoyaltyMaintenanceJob
{
    private readonly ILoyaltyEventGenerationService _service;
    private readonly ILogger<LoyaltyMaintenanceJob> _logger;

    public LoyaltyMaintenanceJob(ILoyaltyEventGenerationService service, ILogger<LoyaltyMaintenanceJob> logger)
    {
        _service = service;
        _logger = logger;
    }

    public async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Iniciando LoyaltyMaintenanceJob.");
        await _service.RunMaintenanceAsync(cancellationToken);
    }
}