using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Dtos;

namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.FileReaders;

public interface IRawFileReader
{
    bool CanRead(string filePath);
    Task<IReadOnlyList<ImportRawRowDto>> ReadAsync(string filePath, CancellationToken cancellationToken);
}