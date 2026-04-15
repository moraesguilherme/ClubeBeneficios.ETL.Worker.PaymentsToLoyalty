namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Interfaces;

public interface IRowMatchingService
{
    Task<int> MatchParsedRowsAsync(CancellationToken cancellationToken);
}