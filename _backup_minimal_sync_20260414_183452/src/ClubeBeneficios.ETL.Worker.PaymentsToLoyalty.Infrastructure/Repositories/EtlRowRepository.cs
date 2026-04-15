using System.Data;
using Dapper;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Interfaces;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Persistence;

namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Repositories;

public class EtlRowRepository : RepositoryBase, IEtlRowRepository
{
    public EtlRowRepository(IDbConnectionFactory connectionFactory) : base(connectionFactory)
    {
    }

    public async Task<long> CreateImportRowAsync(
        Guid batchId,
        int rowNumber,
        string? externalRowKey,
        string rawPayloadJson,
        CancellationToken cancellationToken)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);

        var parameters = new DynamicParameters();
        parameters.Add("@BatchId", batchId);
        parameters.Add("@RowNumber", rowNumber);
        parameters.Add("@ExternalRowKey", externalRowKey);
        parameters.Add("@RawPayloadJson", rawPayloadJson);

        return await connection.ExecuteScalarAsync<long>(
            new CommandDefinition(
                "dbo.usp_etl_import_row_create",
                parameters,
                commandType: CommandType.StoredProcedure,
                cancellationToken: cancellationToken));
    }
}