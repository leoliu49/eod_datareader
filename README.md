# eod_datareader
EOD stock datareader module written in Python. This is a wrapper around the [pandas-datareader](https://pypi.python.org/pypi/pandas-datareader) module.  
This module is capable of collecting EOD stock data, and exporting them as CSV files for persistent storage.

## Getting Started
### Prerequisites
You'll need to have the following packages available to Python 2:
* [pandas](https://pypi.python.org/pypi/pandas/0.18.1)
* [pandas-datareader](https://pypi.python.org/pypi/pandas-datareader)

More prerequisites will be needed as I add more functionality to this!

### Installing
This project is not packagable yet!  
To use it, you'll have to clone the source and use it as an imported module. I'll eventually get to making this distributable...
```
git clone https://github.com/leoliu49/eod_datareader.git
```

To use the module:
```python
from eod_datareader import eod_datareader
...
eod_datareader.get('AAPL', '2017-07-07', '2017-08-08')
```
Data is returned to you in the form of pandas [DataFrames](https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.html).

## Up Next
* Features
  * graphing DataFrames
  * stock data manipulation (e.g. candlestick graphs)
* Infrastructure
  * packaging
  * PyPI distribution once it's near completion
