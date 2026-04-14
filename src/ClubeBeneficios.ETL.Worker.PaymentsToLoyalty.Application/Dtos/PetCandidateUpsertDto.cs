namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Dtos;

public class PetCandidateUpsertDto
{
    public long ImportRowId { get; set; }
    public string PetNameRaw { get; set; } = string.Empty;
    public string? NormalizedPetName { get; set; }
    public Guid? ClientPetId { get; set; }
    public decimal? MatchConfidence { get; set; }
    public bool ReviewRequired { get; set; }
    public bool IsPrimary { get; set; }
}