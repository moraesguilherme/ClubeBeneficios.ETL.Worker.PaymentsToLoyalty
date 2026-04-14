using System.Globalization;
using ClosedXML.Excel;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Dtos;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application.Interfaces;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Configuration;
using ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.FileReaders;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure.Services;

public class RowParserService : IRowParserService
{
    private readonly ILogger<RowParserService> _logger;
    private readonly EtlWorkerOptions _options;
    private readonly IEtlServicePaymentFactRepository _factRepository;
    private readonly IEtlPetCandidateRepository _petCandidateRepository;
    private readonly IHotelWorkbookReader _hotelWorkbookReader;
    private readonly ICrecheWorkbookReader _crecheWorkbookReader;

    public RowParserService(
        ILogger<RowParserService> logger,
        IOptions<EtlWorkerOptions> options,
        IEtlServicePaymentFactRepository factRepository,
        IEtlPetCandidateRepository petCandidateRepository,
        IHotelWorkbookReader hotelWorkbookReader,
        ICrecheWorkbookReader crecheWorkbookReader)
    {
        _logger = logger;
        _options = options.Value;
        _factRepository = factRepository;
        _petCandidateRepository = petCandidateRepository;
        _hotelWorkbookReader = hotelWorkbookReader;
        _crecheWorkbookReader = crecheWorkbookReader;
    }

    public async Task<int> ParsePendingRowsAsync(CancellationToken cancellationToken)
    {
        var imported = 0;

        if (!Directory.Exists(_options.WatchFolderPath))
        {
            return 0;
        }

        var xlsxFiles = Directory.GetFiles(_options.WatchFolderPath, "*.xlsx", SearchOption.TopDirectoryOnly)
            .OrderBy(x => x)
            .ToList();

        foreach (var file in xlsxFiles)
        {
            if (_hotelWorkbookReader.CanRead(file))
            {
                imported += await ParseHotelAsync(file, cancellationToken);
            }
            else if (_crecheWorkbookReader.CanRead(file))
            {
                imported += await ParseCrecheAsync(file, cancellationToken);
            }
        }

        return imported;
    }

    private async Task<int> ParseHotelAsync(string filePath, CancellationToken cancellationToken)
    {
        using var workbook = new XLWorkbook(filePath);
        var sheet = _hotelWorkbookReader.GetWorksheet(workbook);

        if (sheet is null)
        {
            _logger.LogWarning("Aba AGENDA 2026 nÃ£o encontrada em {FilePath}", filePath);
            return 0;
        }

        var lastRow = sheet.LastRowUsed()?.RowNumber() ?? 0;
        if (lastRow < 2)
        {
            return 0;
        }

        var count = 0;

        for (var row = 2; row <= lastRow; row++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var customerName = sheet.Cell(row, 4).GetValue<string>()?.Trim();
            var petName = sheet.Cell(row, 3).GetValue<string>()?.Trim();
            var gross = ParseDecimal(sheet.Cell(row, 8).GetValue<string>());
            var paymentMethod = sheet.Cell(row, 9).GetValue<string>()?.Trim();
            var observation = sheet.Cell(row, 10).GetValue<string>()?.Trim();

            if (string.IsNullOrWhiteSpace(customerName) &&
                string.IsNullOrWhiteSpace(petName) &&
                gross is null)
            {
                continue;
            }

            await _factRepository.UpsertAsync(
                new ServicePaymentFactUpsertDto
                {
                    ImportRowId = row,
                    BatchId = Guid.Empty,
                    SourceFileType = "hotel_agenda",
                    SourceSheetName = "AGENDA 2026",
                    ReferenceYear = 2026,
                    ServiceFamily = "hotel",
                    ServiceType = "hotel",
                    CustomerName = customerName,
                    PetNameRaw = petName,
                    PetCount = SplitPets(petName).Count,
                    GrossAmount = gross,
                    NetAmount = gross,
                    PaymentMethodRaw = paymentMethod,
                    PaymentMethodNormalized = NormalizePaymentMethod(paymentMethod),
                    ObservationRaw = observation
                },
                cancellationToken);

            var pets = SplitPets(petName);
            for (var i = 0; i < pets.Count; i++)
            {
                await _petCandidateRepository.UpsertAsync(
                    new PetCandidateUpsertDto
                    {
                        ImportRowId = row,
                        PetNameRaw = pets[i],
                        NormalizedPetName = pets[i].Trim().ToUpperInvariant(),
                        ReviewRequired = false,
                        IsPrimary = i == 0
                    },
                    cancellationToken);
            }

            count++;
        }

        return count;
    }

    private async Task<int> ParseCrecheAsync(string filePath, CancellationToken cancellationToken)
    {
        using var workbook = new XLWorkbook(filePath);
        var sheets = _crecheWorkbookReader.GetMonthlyWorksheets(workbook);

        var count = 0;

        foreach (var sheet in sheets)
        {
            var lastRow = sheet.LastRowUsed()?.RowNumber() ?? 0;
            if (lastRow < 2)
            {
                continue;
            }

            for (var row = 2; row <= lastRow; row++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var customerName = sheet.Cell(row, 4).GetValue<string>()?.Trim();
                var petName = sheet.Cell(row, 3).GetValue<string>()?.Trim();
                var gross = ParseDecimal(sheet.Cell(row, 8).GetValue<string>());
                var taxi = ParseDecimal(sheet.Cell(row, 9).GetValue<string>());
                var net = ParseDecimal(sheet.Cell(row, 10).GetValue<string>());
                var paymentMethod = sheet.Cell(row, 11).GetValue<string>()?.Trim();
                var observation = sheet.Cell(row, 12).GetValue<string>()?.Trim();

                if (string.IsNullOrWhiteSpace(customerName) &&
                    string.IsNullOrWhiteSpace(petName) &&
                    gross is null &&
                    net is null)
                {
                    continue;
                }

                var month = ResolveMonth(sheet.Name);

                await _factRepository.UpsertAsync(
                    new ServicePaymentFactUpsertDto
                    {
                        ImportRowId = row,
                        BatchId = Guid.Empty,
                        SourceFileType = "creche_mensal",
                        SourceSheetName = sheet.Name,
                        ReferenceYear = 2026,
                        ReferenceMonth = month,
                        ServiceFamily = "creche",
                        ServiceType = "creche",
                        CustomerName = customerName,
                        PetNameRaw = petName,
                        PetCount = SplitPets(petName).Count,
                        GrossAmount = gross,
                        TaxiAmount = taxi,
                        NetAmount = net ?? gross,
                        PaymentMethodRaw = paymentMethod,
                        PaymentMethodNormalized = NormalizePaymentMethod(paymentMethod),
                        ObservationRaw = observation
                    },
                    cancellationToken);

                var pets = SplitPets(petName);
                for (var i = 0; i < pets.Count; i++)
                {
                    await _petCandidateRepository.UpsertAsync(
                        new PetCandidateUpsertDto
                        {
                            ImportRowId = row,
                            PetNameRaw = pets[i],
                            NormalizedPetName = pets[i].Trim().ToUpperInvariant(),
                            ReviewRequired = false,
                            IsPrimary = i == 0
                        },
                        cancellationToken);
                }

                count++;
            }
        }

        return count;
    }

    private static decimal? ParseDecimal(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        value = value.Replace("R$", string.Empty).Trim();

        if (decimal.TryParse(value, NumberStyles.Any, new CultureInfo("pt-BR"), out var result))
            return result;

        return null;
    }

    private static string? NormalizePaymentMethod(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        var normalized = value.Trim().ToLowerInvariant();

        if (normalized.Contains("pix")) return "pix";
        if (normalized.Contains("dinheiro") || normalized.Contains("espÃ©cie") || normalized.Contains("especie")) return "cash";
        if (normalized.Contains("crÃ©dito") || normalized.Contains("credito")) return "credit_card";
        if (normalized.Contains("dÃ©bito") || normalized.Contains("debito")) return "debit_card";
        if (normalized.Contains("boleto")) return "boleto";
        if (normalized.Contains("transfer")) return "bank_transfer";

        return "other";
    }

    private static List<string> SplitPets(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return new List<string>();

        return value
            .Split(new[] { "/", ",", ";" }, StringSplitOptions.RemoveEmptyEntries)
            .Select(x => x.Trim())
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static int? ResolveMonth(string sheetName)
    {
        return sheetName.Trim().ToUpperInvariant() switch
        {
            "JANEIRO" => 1,
            "FEVEREIRO" => 2,
            "MARÃ‡O" => 3,
            "MARCO" => 3,
            "ABRIL" => 4,
            "MAIO" => 5,
            "JUNHO" => 6,
            "JULHO" => 7,
            "AGOSTO" => 8,
            "SETEMBRO" => 9,
            "OUTUBRO" => 10,
            "NOVEMBRO" => 11,
            "DEZEMBRO" => 12,
            _ => null
        };
    }
}