# energy-market-etl-dev

# CLI
Built in CLI provides an interface to parameterize the generated report.  
Below is a list of available parameters:
* **-rt (--report-type)**  - Specifies type of report to be generated
* **-sd (--start-date)**    - Specifies first data snapshot date (in format YYYY-MM-DD)
* **-ed (--end-date)**       - Specifies last data snapshot date (in format YYYY-MM-DD)

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