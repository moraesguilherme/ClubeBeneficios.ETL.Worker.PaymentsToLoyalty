namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Interfaces;

public interface IRowParserService
{
    Task<int> ParsePendingRowsAsync(CancellationToken cancellationToken);
}