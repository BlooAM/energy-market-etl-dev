# energy-market-etl-dev
The package provides functionality for generating reports (by default in CSV format) aggregating data related to 
the energy sector. Currently, three types of reports are implemented, each associated with a different data source:
* **market_data**  - Aggregates data from service: [TGE](https://tge.pl/energia-elektryczna-rdn) (table "Kontrakty godzinowe")
* **system_data**       - Aggregates data from service: [PSE - system](https://www.pse.pl/dane-systemowe/funkcjonowanie-kse/raporty-dobowe-z-pracy-kse/wielkosci-podstawowe) (main table)
* **system_units_data**    - Aggregates data from service: [PSE - units](https://www.pse.pl/dane-systemowe/funkcjonowanie-kse/raporty-dobowe-z-pracy-kse/generacja-mocy-jednostek-wytworczych) (main table)

Each report can be parameterized to any date range (from `start_date` to `end_date` - see CLI documentation below) 

The generated reports are saved in the `reports` folder with the predefined name `{report_type}_{start_date}_{end_date}.csv`

# CLI
Built in CLI provides an interface to parameterize the generated report.  
Below is a list of available parameters:
* **-rt (--report-type)**  - Specifies type of report to be generated
* **-ed (--end-date)**       - Specifies last data snapshot date (format: YYYY-MM-DD)
* **-sd (--start-date)**    - Specifies first data snapshot date (format: YYYY-MM-DD)
* **-vb (--verbose)**       - Optional, indicates how detailed information is provided via log

Available commands with associated formats can be obtained by running flag `-h` (or `--help`) next to the core command

```text
$ python cli.py -h
```
or
```text
$ python cli.py --help
```

This will output to your shell the following documentation.

```text
usage: cli.py [-h] -rt {market_data,system_data,system_units_data} -sd START_DATE -ed END_DATE [-vb {0,1}]

Generate report regarding energy sector data

options:
  -h, --help            show this help message and exit
  -rt {market_data,system_data,system_units_data}, --report-type {market_data,system_data,system_units_data}
                        Report type
  -sd START_DATE, --start-date START_DATE
                        First data snapshot date (YYYY-MM-DD format)
  -ed END_DATE, --end-date END_DATE
                        Last data snapshot date (YYYY-MM-DD format)
  -vb {0,1}, --verbose {0,1}
                        Verbose parameter - higher value indicates more detailed log information
```

**Example**

The report for TGE data (electricity market data) for the period from 01-10-2022 to 15-01-2023 corresponds to the 
following parameterization (note usage of `--verbose` parameter - it is useful for longer time horizons, for which 
extraction may take some time:
```text
$ python cli.py -rt market_data -sd 2022-10-1 -ed 2023-1-15 -vb 1
```
The report named `market_data_2022-10-01_2023-01-15.csv` will be saved in the folder `reports`.