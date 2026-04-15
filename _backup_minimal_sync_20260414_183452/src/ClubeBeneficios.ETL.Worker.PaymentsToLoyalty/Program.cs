using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Interfaces;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Configuration;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.FileReaders;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Jobs;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Persistence;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Repositories;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Services;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.HostedServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<EtlWorkerOptions>(
    builder.Configuration.GetSection("EtlWorker"));

builder.Services.AddSingleton<IDbConnectionFactory, SqlConnectionFactory>();

builder.Services.AddScoped<IEtlBatchRepository, EtlBatchRepository>();
builder.Services.AddScoped<IEtlRowRepository, EtlRowRepository>();
builder.Services.AddScoped<IEtlProcessingRunRepository, EtlProcessingRunRepository>();
builder.Services.AddScoped<IEtlServicePaymentFactRepository, EtlServicePaymentFactRepository>();
builder.Services.AddScoped<IEtlPetCandidateRepository, EtlPetCandidateRepository>();

builder.Services.AddScoped<IFileImportService, FileImportService>();
builder.Services.AddScoped<IRowParserService, RowParserService>();
builder.Services.AddScoped<IRowMatchingService, RowMatchingService>();
builder.Services.AddScoped<ILoyaltyEventGenerationService, LoyaltyEventGenerationService>();

builder.Services.AddScoped<IHotelWorkbookReader, HotelWorkbookReader>();
builder.Services.AddScoped<ICrecheWorkbookReader, CrecheWorkbookReader>();

builder.Services.AddScoped<FileIngestionJob>();
builder.Services.AddScoped<RowParsingJob>();
builder.Services.AddScoped<RowMatchingJob>();
builder.Services.AddScoped<LoyaltyGenerationJob>();
builder.Services.AddScoped<LoyaltyMaintenanceJob>();

builder.Services.AddHostedService<PipelineHostedService>();

var host = builder.Build();
await host.RunAsync();