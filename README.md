# ozgrav-dp2-workshop

## Getting the data
Choose your own adventure:
- If you have a Rubin RSP account, you can generate your own filtered dataset using SQL queries with a notebook here: https://nb.lsst.io/index.html
    - You can also use the API (https://data.lsst.cloud/api-aspect) but I've found its much slower than filtering the data on an RSP notebook and has more stringent row limits.
    - The ideal solution is an Independent Data Access Center, but the OzStar IDAC does not yet have the DP2 data.
- Here (https://drive.google.com/drive/folders/1ZU-3RhBylzweO07juBsYvTvAKd-9C_gk?usp=sharing) is some data that I have prepared earlier using a notebook with the following:
```python
from lsst.rsp import RSPDiscovery
import pyvo
import numpy as np
from astropy.table import Table
import pandas as pd
from glob import glob

discovery = RSPDiscovery("dp2")
tap_service = discovery.get_tap_client()

# Get all DP2 Visits (for MJDs primarily)
q0 = """
SELECT
    v.*
FROM dp2.Visit AS v
"""
job0 = tap_service.run_async(q2)
visit = job0.to_table().to_pandas()
visit.to_parquet("Visit.parquet",index=False)
del visit

# Initial query to get suitable DIA objects
q1 = """
SELECT dd.*
FROM dp2.DiaSource AS s
JOIN dp2.DiaObject AS dd ON dd.diaObjectId = s.diaObjectId
WHERE dd.r_psfFluxNdata > 3
  AND dd.r_psfFluxMax/dd.r_psfFluxMaxSlope > 0.1
  AND s.snr > 10
  AND s.band = 'r'
  AND s.isDipole = 0
  AND s.shape_flag = 0
  AND s.pixelFlags = 0
  AND s.glint_trail = 0
  AND s.psfFlux_flag = 0
"""
job1 = tap_service.run_async(q1)
diaobject = job1.to_table().to_pandas()
diaobject.to_parquet("DiaObject.parquet",index=False)

# Get DIA forced photometry by uploading a table containing all the object ids we want (this part takes ages)
id_table = Table({'diaObjectId': list(diaobject['diaObjectId'])})
del diaobject

q2 = """
SELECT
    f.*
FROM dp2.ForcedSourceOnDiaObject AS f
JOIN TAP_UPLOAD.id_table AS u ON f.diaObjectId = u.diaObjectId
WHERE f.pixelFlags_bad = 0
AND f.pixelFlags_suspect = 0
AND f.invalidPsfFlag = 0
AND f.psfDiffFlux_flag = 0
"""
nchunks = 10
bin_size = int(len(id_table)/(nchunks-1))

result_arr = [None]*nchunks
os.makedirs("sn_data/",exist_ok=True)
for i in range(nchunks):
    if os.path.exists(f"sn_data/ForcedSourceOnDiaObject_{i}.parquet"):
        continue
    print(f"running chunk {i} of {nchunks}")
    lo_idx = i*bin_size
    hi_idx = (i+1)*bin_size if (i+1)*bin_size < len(id_table) else None

    job2 = tap_service.run_async(q2, uploads={"id_table": id_table[lo_idx:hi_idx]})
    job2.to_table().to_pandas().to_parquet(f"sn_data/ForcedSourceOnDiaObject_{i}.parquet")

result_arr = [pd.read_parquet(file) for file in glob("sn_data/ForcedSourceOnDiaObject_*.parquet")]
pd.concat(result_arr).to_parquet("sn_data/ForcedSourceOnDiaObject.parquet",index=False)
```
> **Note:** This is quite narrowly filtered for fast(ish) transients which have a fade rate of >10% per day. To look for slower evolving things or variable stars, you might want to do your own query rather than using the data on the google drive.

## Preparing the python environment
