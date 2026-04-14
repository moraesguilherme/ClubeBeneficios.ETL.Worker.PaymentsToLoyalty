using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Dtos;

namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Interfaces;

public interface IEtlPetCandidateRepository
{
    Task<long> UpsertAsync(PetCandidateUpsertDto dto, CancellationToken cancellationToken);
}