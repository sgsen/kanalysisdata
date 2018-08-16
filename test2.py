# Example: loading IMF data into pandas

# Import libraries
import requests
import pandas as pd

# URL for the IMF JSON Restful Web Service,
# IFS database, and Australian export prices series
url = 'http://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/IFS/Q.AU.PXP_IX.?startPeriod=1957&endPeriod=2016'

# Get data from the above URL using the requests package
data = requests.get(url).json()

# Load data into a pandas dataframe
auxp = pd.DataFrame(data['CompactData']['DataSet']['Series']['Obs'])

# Show the last five observiations
print(auxp.tail())

url2 = 'http://dataservices.imf.org/REST/SDMX_JSON.svc/DataFlow'

# Get data from the above URL using the requests package
data2 = requests.get(url2).json()

print(data2)
