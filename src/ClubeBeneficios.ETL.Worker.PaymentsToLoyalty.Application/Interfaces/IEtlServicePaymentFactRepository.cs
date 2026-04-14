using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Dtos;

namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Interfaces;

public interface IEtlServicePaymentFactRepository
{
    Task<long> UpsertAsync(ServicePaymentFactUpsertDto dto, CancellationToken cancellationToken);
}