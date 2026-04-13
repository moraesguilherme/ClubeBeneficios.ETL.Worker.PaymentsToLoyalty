namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Interfaces;

public interface IEtlBatchRepository
{
    Task<Guid> CreateBatchAsync(
        string sourceName,
        string sourceType,
        string? fileName,
        string? fileHash,
        Guid? createdByUserId,
        string? notes,
        CancellationToken cancellationToken);

    Task SetBatchStatusAsync(
        Guid batchId,
        string status,
        int totalRows,
        int processedRows,
        int successRows,
        int errorRows,
        string? notes,
        CancellationToken cancellationToken);
}