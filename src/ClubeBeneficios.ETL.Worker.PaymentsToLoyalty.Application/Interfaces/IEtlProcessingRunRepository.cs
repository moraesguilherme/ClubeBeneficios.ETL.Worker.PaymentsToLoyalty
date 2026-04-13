namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Interfaces;

public interface IEtlProcessingRunRepository
{
    Task<Guid> CreateRunAsync(
        Guid? batchId,
        string runType,
        string status,
        CancellationToken cancellationToken);

    Task FinishRunAsync(
        Guid id,
        string status,
        int processedItems,
        int successItems,
        int errorItems,
        string? logSummary,
        CancellationToken cancellationToken);
}