using System.Data;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Persistence;

namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Repositories;

public abstract class RepositoryBase
{
    private readonly IDbConnectionFactory _connectionFactory;

    protected RepositoryBase(IDbConnectionFactory connectionFactory)
    {
        _connectionFactory = connectionFactory;
    }

    protected Task<IDbConnection> OpenConnectionAsync(CancellationToken cancellationToken)
    {
        return _connectionFactory.CreateOpenConnectionAsync(cancellationToken);
    }
}