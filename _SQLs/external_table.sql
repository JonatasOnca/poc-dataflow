--  Copyright 2025 TecOnca Data Solutions.


CREATE OR REPLACE EXTERNAL TABLE `abemcomum-saev-prod.landing_zone.raca`
OPTIONS (
  uris = ['gs://abemcomum-saev-prod/landing_zone/raca/*'],
  format = 'PARQUET'
);