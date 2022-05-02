# Proteus
This project aim at providing tools needed for everyday activities of data scientists and engineers:
- Connectors for internal and external API services
- Secure secret handlers for various remote storages
- Logging framework
- Metrics reporting framework
- Storage drivers for various clouds and storage types

## Delta Lake

This module provides basic Delta Lake operations without Spark session, based on [delta-rs](https://github.com/delta-io/delta-rs) project.

Please refer to the [module](proteus/storage/delta_lake/README.md) documentation for examples.

## Secret Storages

Please refer to the [module](proteus/storage/secrets/README.md) documentation for examples.
