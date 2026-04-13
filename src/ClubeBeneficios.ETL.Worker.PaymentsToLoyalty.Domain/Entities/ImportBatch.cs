namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Domain.Entities;

public class ImportBatch
{
    public Guid Id { get; set; }
    public string SourceName { get; set; } = string.Empty;
    public string SourceType { get; set; } = string.Empty;
    public string? FileName { get; set; }
    public string? FileHash { get; set; }
    public string Status { get; set; } = "pending";
    public int TotalRows { get; set; }
    public int ProcessedRows { get; set; }
    public int SuccessRows { get; set; }
    public int ErrorRows { get; set; }
    public DateTime StartedAt { get; set; }
    public DateTime? FinishedAt { get; set; }
}