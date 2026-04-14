using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Interfaces;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Configuration;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Jobs;
using Microsoft.Extensions.Options;

namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.HostedServices;

public class PipelineHostedService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<PipelineHostedService> _logger;
    private readonly EtlWorkerOptions _options;

    public PipelineHostedService(
        IServiceProvider serviceProvider,
        ILogger<PipelineHostedService> logger,
        IOptions<EtlWorkerOptions> options)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
        _options = options.Value;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("ETL Worker iniciado em modo: {Mode}", _options.Mode);

        while (!stoppingToken.IsCancellationRequested)
        {
            using var scope = _serviceProvider.CreateScope();

            if (string.Equals(_options.Mode, "watch", StringComparison.OrdinalIgnoreCase))
            {
                var ingestionJob = scope.ServiceProvider.GetRequiredService<FileIngestionJob>();
                await ingestionJob.ExecuteAsync(stoppingToken);
            }
            else if (
                string.Equals(_options.Mode, "import-file", StringComparison.OrdinalIgnoreCase) &&
                !string.IsNullOrWhiteSpace(_options.FilePath))
            {
                var importService = scope.ServiceProvider.GetRequiredService<IFileImportService>();
                await importService.ImportFileAsync(_options.FilePath!, stoppingToken);
            }

            if (_options.EnableParsingJob)
            {
                var parsingJob = scope.ServiceProvider.GetRequiredService<RowParsingJob>();
                await parsingJob.ExecuteAsync(stoppingToken);
            }

            if (_options.EnableMatchingJob)
            {
                var matchingJob = scope.ServiceProvider.GetRequiredService<RowMatchingJob>();
                await matchingJob.ExecuteAsync(stoppingToken);
            }

            if (_options.EnableLoyaltyGenerationJob)
            {
                var loyaltyJob = scope.ServiceProvider.GetRequiredService<LoyaltyGenerationJob>();
                await loyaltyJob.ExecuteAsync(stoppingToken);
            }

            if (_options.EnableMaintenanceJob)
            {
                var maintenanceJob = scope.ServiceProvider.GetRequiredService<LoyaltyMaintenanceJob>();
                await maintenanceJob.ExecuteAsync(stoppingToken);
            }

            await Task.Delay(TimeSpan.FromSeconds(_options.PollingIntervalSeconds), stoppingToken);
        }
    }
}