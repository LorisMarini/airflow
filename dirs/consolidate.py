from skepsi.utils.utils import *


must_have = [env_get("ETL_HOME_TEMP_OBJECTS"),
             env_get("ETL_HOME_TEMP_LOGS")]

for path in must_have:
    os.makedirs(path, exist_ok=True)
