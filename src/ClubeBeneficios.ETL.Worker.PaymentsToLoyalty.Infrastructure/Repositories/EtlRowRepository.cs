using System.Data;
using Dapper;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Dtos;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Interfaces;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Persistence;

namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Repositories;

public class EtlRowRepository : RepositoryBase, IEtlRowRepository
{
    public EtlRowRepository(IDbConnectionFactory connectionFactory) : base(connectionFactory)
    {
    }

    public async Task<long> CreateImportRowAsync(
        ImportRowCreateDto dto,
        CancellationToken cancellationToken)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);

        var parameters = new DynamicParameters();
        parameters.Add("@BatchId", dto.BatchId);
        parameters.Add("@RowNumber", dto.RowNumber);
        parameters.Add("@ExternalRowKey", dto.ExternalRowKey);
        parameters.Add("@RawPayloadJson", dto.RawPayloadJson);

        parameters.Add("@OccurredAt", dto.OccurredAt);
        parameters.Add("@CompetenceDate", dto.CompetenceDate);

        parameters.Add("@CustomerNameRaw", dto.CustomerNameRaw);
        parameters.Add("@CustomerDocumentRaw", dto.CustomerDocumentRaw);
        parameters.Add("@CustomerEmailRaw", dto.CustomerEmailRaw);
        parameters.Add("@CustomerPhoneRaw", dto.CustomerPhoneRaw);

        parameters.Add("@PetNameRaw", dto.PetNameRaw);
        parameters.Add("@PartnerNameRaw", dto.PartnerNameRaw);

        parameters.Add("@ServiceTypeRaw", dto.ServiceTypeRaw);
        parameters.Add("@PlanNameRaw", dto.PlanNameRaw);
        parameters.Add("@PackageNameRaw", dto.PackageNameRaw);
        parameters.Add("@LodgingTypeRaw", dto.LodgingTypeRaw);

        parameters.Add("@PaymentMethodRaw", dto.PaymentMethodRaw);
        parameters.Add("@PaymentMethodNormalized", dto.PaymentMethodNormalized);
        parameters.Add("@PaymentStatusRaw", dto.PaymentStatusRaw);
        parameters.Add("@PaymentStatusNormalized", dto.PaymentStatusNormalized);

        parameters.Add("@GrossAmount", dto.GrossAmount);
        parameters.Add("@DiscountAmount", dto.DiscountAmount);
        parameters.Add("@NetAmount", dto.NetAmount);
        parameters.Add("@TaxiAmount", dto.TaxiAmount);
        parameters.Add("@Quantity", dto.Quantity);

        parameters.Add("@StartDate", dto.StartDate);
        parameters.Add("@EndDate", dto.EndDate);

        parameters.Add("@DescriptionRaw", dto.DescriptionRaw);
        parameters.Add("@ObservationRaw", dto.ObservationRaw);

        parameters.Add("@SourceSheetName", dto.SourceSheetName);
        parameters.Add("@SourceSheetGroup", dto.SourceSheetGroup);
        parameters.Add("@ReferenceYear", dto.ReferenceYear);
        parameters.Add("@ReferenceMonth", dto.ReferenceMonth);
        parameters.Add("@SourceFileType", dto.SourceFileType);

        var row = await connection.QuerySingleAsync(
            new CommandDefinition(
                "dbo.usp_etl_import_row_create",
                parameters,
                commandType: CommandType.StoredProcedure,
                cancellationToken: cancellationToken));

        return (long)row.id;
    }
}