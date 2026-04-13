namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Interfaces;

public interface IFileImportService
{
    Task<Guid?> ImportFileAsync(string filePath, CancellationToken cancellationToken);
    Task<int> ImportPendingFilesAsync(CancellationToken cancellationToken);
}