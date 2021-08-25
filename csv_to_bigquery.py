from google.colab import auth
auth.authenticate_user()
print('Authenticated')

import pandas as pd
from google.colab import syntax
from google.colab import drive

drive.mount('/content/gdrive')

%cd /content/gdrive/MyDrive/my_files


project_id = 'xxxxx-xxxxx-xxxxx'

#open file csv
df = pd.read_csv('Mini Survey Structures (Responses).csv')
#cleaning columns
df.columns = df.columns.str.replace(' ', '_',)
df.columns = df.columns.str.replace(r'\([^)]*\)', '')
df.columns = df.columns.str.replace(r'\?', '')

df.to_gbq(destination_table='survey.mini_survey', project_id=project_id, chunksize=None, if_exists='append')

#query from table mini_survey
query = syntax.sql('''
SELECT
  What_is_your_level_of_understanding_on_Python,COUNT(*) as total_rows
FROM
 `survey.mini_survey` group by What_is_your_level_of_understanding_on_Python
''')
res_query=pd.io.gbq.read_gbq(query, project_id=project_id, dialect='standard')

#save result query to new table understand_python
res_query.to_gbq(destination_table='survey.understand_python', project_id=project_id, chunksize=None, if_exists='append')
