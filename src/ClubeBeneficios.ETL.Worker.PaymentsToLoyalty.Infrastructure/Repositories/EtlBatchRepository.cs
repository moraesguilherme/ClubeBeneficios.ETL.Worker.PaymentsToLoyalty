using System.Data;
using Dapper;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Interfaces;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Persistence;

namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Repositories;

public class EtlBatchRepository : RepositoryBase, IEtlBatchRepository
{
    public EtlBatchRepository(IDbConnectionFactory connectionFactory) : base(connectionFactory)
    {
    }

    public async Task<Guid> CreateBatchAsync(
        string sourceName,
        string sourceType,
        string? fileName,
        string? fileHash,
        Guid? createdByUserId,
        string? notes,
        CancellationToken cancellationToken)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);

        var parameters = new DynamicParameters();
        parameters.Add("@SourceName", sourceName);
        parameters.Add("@SourceType", sourceType);
        parameters.Add("@FileName", fileName);
        parameters.Add("@FileHash", fileHash);
        parameters.Add("@CreatedByUserId", createdByUserId);
        parameters.Add("@Notes", notes);

        return await connection.ExecuteScalarAsync<Guid>(
            new CommandDefinition(
                "dbo.usp_etl_import_batch_create",
                parameters,
                commandType: CommandType.StoredProcedure,
                cancellationToken: cancellationToken));
    }

    public async Task SetBatchStatusAsync(
        Guid batchId,
        string status,
        int totalRows,
        int processedRows,
        int successRows,
        int errorRows,
        string? notes,
        CancellationToken cancellationToken)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);

        var parameters = new DynamicParameters();
        parameters.Add("@Id", batchId);
        parameters.Add("@Status", status);
        parameters.Add("@TotalRows", totalRows);
        parameters.Add("@ProcessedRows", processedRows);
        parameters.Add("@SuccessRows", successRows);
        parameters.Add("@ErrorRows", errorRows);
        parameters.Add("@Notes", notes);

        await connection.ExecuteAsync(
            new CommandDefinition(
                "dbo.usp_etl_import_batch_set_status",
                parameters,
                commandType: CommandType.StoredProcedure,
                cancellationToken: cancellationToken));
    }
}