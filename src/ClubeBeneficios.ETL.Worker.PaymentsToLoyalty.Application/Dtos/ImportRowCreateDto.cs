namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Dtos;

public class ImportRowCreateDto
{
    public Guid BatchId { get; set; }
    public int RowNumber { get; set; }
    public string? ExternalRowKey { get; set; }
    public string RawPayloadJson { get; set; } = string.Empty;

    public DateTime? OccurredAt { get; set; }
    public DateTime? CompetenceDate { get; set; }

    public string? CustomerNameRaw { get; set; }
    public string? CustomerDocumentRaw { get; set; }
    public string? CustomerEmailRaw { get; set; }
    public string? CustomerPhoneRaw { get; set; }

    public string? PetNameRaw { get; set; }
    public string? PartnerNameRaw { get; set; }

    public string? ServiceTypeRaw { get; set; }
    public string? PlanNameRaw { get; set; }
    public string? PackageNameRaw { get; set; }
    public string? LodgingTypeRaw { get; set; }

    public string? PaymentMethodRaw { get; set; }
    public string? PaymentMethodNormalized { get; set; }
    public string? PaymentStatusRaw { get; set; }
    public string? PaymentStatusNormalized { get; set; }

    public decimal? GrossAmount { get; set; }
    public decimal? DiscountAmount { get; set; }
    public decimal? NetAmount { get; set; }
    public decimal? TaxiAmount { get; set; }
    public decimal? Quantity { get; set; }

    public DateTime? StartDate { get; set; }
    public DateTime? EndDate { get; set; }

    public string? DescriptionRaw { get; set; }
    public string? ObservationRaw { get; set; }

    public string? SourceSheetName { get; set; }
    public string? SourceSheetGroup { get; set; }
    public int? ReferenceYear { get; set; }
    public int? ReferenceMonth { get; set; }
    public string? SourceFileType { get; set; }
}