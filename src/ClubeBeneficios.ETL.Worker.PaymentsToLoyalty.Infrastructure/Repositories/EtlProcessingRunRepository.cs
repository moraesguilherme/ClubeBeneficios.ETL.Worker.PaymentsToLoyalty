using System.Data;
using Dapper;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Interfaces;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Persistence;

namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Repositories;

public class EtlProcessingRunRepository : RepositoryBase, IEtlProcessingRunRepository
{
    public EtlProcessingRunRepository(IDbConnectionFactory connectionFactory) : base(connectionFactory)
    {
    }

    public async Task<Guid> CreateRunAsync(
        Guid? batchId,
        string runType,
        string status,
        CancellationToken cancellationToken)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);

        var parameters = new DynamicParameters();
        parameters.Add("@BatchId", batchId);
        parameters.Add("@RunType", runType);
        parameters.Add("@Status", status);

        return await connection.ExecuteScalarAsync<Guid>(
            new CommandDefinition(
                "dbo.usp_etl_processing_run_create",
                parameters,
                commandType: CommandType.StoredProcedure,
                cancellationToken: cancellationToken));
    }

    public async Task FinishRunAsync(
        Guid id,
        string status,
        int processedItems,
        int successItems,
        int errorItems,
        string? logSummary,
        CancellationToken cancellationToken)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);

        var parameters = new DynamicParameters();
        parameters.Add("@Id", id);
        parameters.Add("@Status", status);
        parameters.Add("@ProcessedItems", processedItems);
        parameters.Add("@SuccessItems", successItems);
        parameters.Add("@ErrorItems", errorItems);
        parameters.Add("@LogSummary", logSummary);

        await connection.ExecuteAsync(
            new CommandDefinition(
                "dbo.usp_etl_processing_run_finish",
                parameters,
                commandType: CommandType.StoredProcedure,
                cancellationToken: cancellationToken));
    }
}