COPY INTO @TELECOM_ANALYSIS.OUTPUT.ext_unload_stage/output_folder/output.csv
from TELECOM_ANALYSIS.OUTPUT.OUTPUT
OVERWRITE=TRUE
SINGLE=TRUE;