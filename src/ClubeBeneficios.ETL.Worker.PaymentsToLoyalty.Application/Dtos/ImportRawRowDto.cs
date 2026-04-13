namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Dtos;

public class ImportRawRowDto
{
    public int RowNumber { get; set; }
    public string? ExternalRowKey { get; set; }
    public string RawPayloadJson { get; set; } = string.Empty;
}