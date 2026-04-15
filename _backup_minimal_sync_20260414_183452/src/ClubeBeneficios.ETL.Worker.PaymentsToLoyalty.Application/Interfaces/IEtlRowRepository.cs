namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Interfaces;

public interface IEtlRowRepository
{
    Task<long> CreateImportRowAsync(
        Guid batchId,
        int rowNumber,
        string? externalRowKey,
        string rawPayloadJson,
        CancellationToken cancellationToken);
}