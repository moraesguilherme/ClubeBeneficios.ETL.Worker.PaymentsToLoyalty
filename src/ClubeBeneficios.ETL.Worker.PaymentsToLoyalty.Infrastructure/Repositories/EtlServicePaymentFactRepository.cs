using System.Data;
using Dapper;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Dtos;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Interfaces;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Persistence;

namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Repositories;

public class EtlServicePaymentFactRepository : RepositoryBase, IEtlServicePaymentFactRepository
{
    public EtlServicePaymentFactRepository(IDbConnectionFactory connectionFactory) : base(connectionFactory)
    {
    }

    public async Task<long> UpsertAsync(ServicePaymentFactUpsertDto dto, CancellationToken cancellationToken)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);

        var parameters = new DynamicParameters();
        parameters.Add("@ImportRowId", dto.ImportRowId);
        parameters.Add("@BatchId", dto.BatchId);
        parameters.Add("@SourceFileType", dto.SourceFileType);
        parameters.Add("@SourceSheetName", dto.SourceSheetName);
        parameters.Add("@ReferenceYear", dto.ReferenceYear);
        parameters.Add("@ReferenceMonth", dto.ReferenceMonth);
        parameters.Add("@ServiceFamily", dto.ServiceFamily);
        parameters.Add("@ServiceType", dto.ServiceType);
        parameters.Add("@PlanName", dto.PlanName);
        parameters.Add("@PackageName", dto.PackageName);
        parameters.Add("@PaymentStatusRaw", dto.PaymentStatusRaw);
        parameters.Add("@PaymentStatusNormalized", dto.PaymentStatusNormalized);
        parameters.Add("@PaymentMethodRaw", dto.PaymentMethodRaw);
        parameters.Add("@PaymentMethodNormalized", dto.PaymentMethodNormalized);
        parameters.Add("@CustomerName", dto.CustomerName);
        parameters.Add("@CustomerDocumentRaw", dto.CustomerDocumentRaw);
        parameters.Add("@CustomerDocumentNormalized", dto.CustomerDocumentNormalized);
        parameters.Add("@CustomerPhoneRaw", dto.CustomerPhoneRaw);
        parameters.Add("@CustomerPhoneNormalized", dto.CustomerPhoneNormalized);
        parameters.Add("@PetNameRaw", dto.PetNameRaw);
        parameters.Add("@PetCount", dto.PetCount);
        parameters.Add("@GrossAmount", dto.GrossAmount);
        parameters.Add("@DiscountAmount", dto.DiscountAmount);
        parameters.Add("@NetAmount", dto.NetAmount);
        parameters.Add("@TaxiAmount", dto.TaxiAmount);
        parameters.Add("@Quantity", dto.Quantity);
        parameters.Add("@OccurredAt", dto.OccurredAt);
        parameters.Add("@StartDate", dto.StartDate);
        parameters.Add("@EndDate", dto.EndDate);
        parameters.Add("@CompetenceDate", dto.CompetenceDate);
        parameters.Add("@DescriptionRaw", dto.DescriptionRaw);
        parameters.Add("@ObservationRaw", dto.ObservationRaw);

        return await connection.ExecuteScalarAsync<long>(
            new CommandDefinition(
                "dbo.usp_etl_service_payment_fact_upsert",
                parameters,
                commandType: CommandType.StoredProcedure,
                cancellationToken: cancellationToken));
    }
}