## iNat_Parquet_Fix_Dates

Ryan Hull with help from Leeya Nagpal & Laura Pollock Quantitative Biodiversity Lab, McGill University July 2025

## Raison-d'être:

Large iNaturalist parquet files for observation data don't contain a standardized date column

## What it does

This code creates new date columns with standardized format. Certain observation entries have no date to start with, and others may be incomplete - these were not handled.

Certain ambiguous dates exist - month and date placement cannot be known for sure when neither exceeds 12. However, among clear cases where the date's placement can be known as the date \> 12, a strong trend can be observed: the month appears first more than 99.9% of the time. The code includes a calculation of the approximate error rate in the month-first assumption for ambiguous dates.

## Usage

The code was written for a subset of around 480,000 iNat observations - only the mammal observations in Canada. For different subsets, the checks may need to be modified, hence it is better to run the code line-by-line and check the outputs.
