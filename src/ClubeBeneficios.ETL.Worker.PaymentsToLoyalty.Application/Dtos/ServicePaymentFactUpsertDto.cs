namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Dtos;

public class ServicePaymentFactUpsertDto
{
    public long ImportRowId { get; set; }
    public Guid BatchId { get; set; }
    public string SourceFileType { get; set; } = string.Empty;
    public string? SourceSheetName { get; set; }
    public int? ReferenceYear { get; set; }
    public int? ReferenceMonth { get; set; }
    public string ServiceFamily { get; set; } = string.Empty;
    public string ServiceType { get; set; } = string.Empty;
    public string? PlanName { get; set; }
    public string? PackageName { get; set; }
    public string? PaymentStatusRaw { get; set; }
    public string? PaymentStatusNormalized { get; set; }
    public string? PaymentMethodRaw { get; set; }
    public string? PaymentMethodNormalized { get; set; }
    public string? CustomerName { get; set; }
    public string? CustomerDocumentRaw { get; set; }
    public string? CustomerDocumentNormalized { get; set; }
    public string? CustomerPhoneRaw { get; set; }
    public string? CustomerPhoneNormalized { get; set; }
    public string? PetNameRaw { get; set; }
    public int? PetCount { get; set; }
    public decimal? GrossAmount { get; set; }
    public decimal? DiscountAmount { get; set; }
    public decimal? NetAmount { get; set; }
    public decimal? TaxiAmount { get; set; }
    public decimal? Quantity { get; set; }
    public DateTime? OccurredAt { get; set; }
    public DateTime? StartDate { get; set; }
    public DateTime? EndDate { get; set; }
    public DateTime? CompetenceDate { get; set; }
    public string? DescriptionRaw { get; set; }
    public string? ObservationRaw { get; set; }
}