using System.Data;
using Dapper;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Dtos;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Interfaces;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Persistence;

namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Repositories;

public class EtlPetCandidateRepository : RepositoryBase, IEtlPetCandidateRepository
{
    public EtlPetCandidateRepository(IDbConnectionFactory connectionFactory) : base(connectionFactory)
    {
    }

    public async Task<long> UpsertAsync(PetCandidateUpsertDto dto, CancellationToken cancellationToken)
    {
        using var connection = await OpenConnectionAsync(cancellationToken);

        var parameters = new DynamicParameters();
        parameters.Add("@ImportRowId", dto.ImportRowId);
        parameters.Add("@PetNameRaw", dto.PetNameRaw);
        parameters.Add("@NormalizedPetName", dto.NormalizedPetName);
        parameters.Add("@ClientPetId", dto.ClientPetId);
        parameters.Add("@MatchConfidence", dto.MatchConfidence);
        parameters.Add("@ReviewRequired", dto.ReviewRequired);
        parameters.Add("@IsPrimary", dto.IsPrimary);

        return await connection.ExecuteScalarAsync<long>(
            new CommandDefinition(
                "dbo.usp_etl_import_row_pet_candidate_upsert",
                parameters,
                commandType: CommandType.StoredProcedure,
                cancellationToken: cancellationToken));
    }
}