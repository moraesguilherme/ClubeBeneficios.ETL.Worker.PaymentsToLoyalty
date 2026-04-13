using System.Data;

namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Persistence;

public interface IDbConnectionFactory
{
    Task<IDbConnection> CreateOpenConnectionAsync(CancellationToken cancellationToken);
}