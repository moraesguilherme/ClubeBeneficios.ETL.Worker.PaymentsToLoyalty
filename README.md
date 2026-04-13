# ClubeBeneficios.ETL.Worker.PaymentsToLoyalty

## Estrutura inicial

- ClubeBeneficios.ETL.Worker.PaymentsToLoyalty
- ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Application
- ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Domain
- ClubeBeneficios.ETL.Worker.PaymentsToLoyalty.Infrastructure

## Objetivo do worker

Este worker foi criado para:

- ingerir arquivos locais (.csv / .xlsx)
- registrar batches e linhas de staging
- executar parse
- executar matching
- gerar eventos de loyalty
- preparar a evoluÃ§Ã£o futura para integraÃ§Ã£o por API

## Modos previstos

- watch
- import-file

## PrÃ³ximos passos

1. Implementar leitura real de CSV/XLSX
2. Ligar repositÃ³rios nas procedures do banco
3. Implementar parse das linhas
4. Implementar matching
5. Implementar geraÃ§Ã£o de eventos de fidelidade