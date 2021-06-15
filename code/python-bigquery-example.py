from google.colab import auth
from google.cloud import bigquery
import pydata_google_auth

auth.authenticate_user()

project_id = 'thanyaboon-bluepi-de-exam'
!gcloud config set project {project_id}
credentials = pydata_google_auth.get_user_credentials(
    ['https://www.googleapis.com/auth/bigquery'],
)

query = '''insert into DATATANK.test
            VALUES ("TEST02","BLUEPI2","EXAMS")'''

client = bigquery.Client(project=project_id, credentials=credentials)
df = client.query(query).to_dataframe()