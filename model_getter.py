import gdown
import os

# TODO move model link id to envirinment variables
url = os.getenv('MODEL_URL')

output = 'churn_f24_v4.sav'
gdown.download(url, output, quiet=True)