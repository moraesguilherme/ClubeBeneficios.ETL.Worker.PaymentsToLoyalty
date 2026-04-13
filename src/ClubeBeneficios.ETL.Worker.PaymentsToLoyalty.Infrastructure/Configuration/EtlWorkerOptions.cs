namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Configuration;

public class EtlWorkerOptions
{
    public string Mode { get; set; } = "watch";
    public string WatchFolderPath { get; set; } = @"C:\ETL\inbound";
    public string ProcessedFolderPath { get; set; } = @"C:\ETL\processed";
    public string ErrorFolderPath { get; set; } = @"C:\ETL\error";
    public string? FilePath { get; set; }
    public int PollingIntervalSeconds { get; set; } = 30;
    public int TopRowsPerCycle { get; set; } = 100;
    public bool EnableParsingJob { get; set; } = true;
    public bool EnableMatchingJob { get; set; } = true;
    public bool EnableLoyaltyGenerationJob { get; set; } = true;
    public bool EnableMaintenanceJob { get; set; } = true;
}