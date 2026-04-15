namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Interfaces;

public interface ILoyaltyEventGenerationService
{
    Task<int> GenerateEventsAsync(CancellationToken cancellationToken);
    Task RunMaintenanceAsync(CancellationToken cancellationToken);
}