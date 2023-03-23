from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable
from datetime import timedelta
import requests
import pendulum
import json
import csv
import pandas as pd
import configparser


with DAG('matches_history',
         start_date = pendulum.datetime(2021, 6, 13, tz = "UTC"),
         schedule = '0 0 * * 1',
         default_args = {'retries': 10,
                         'retry_delay': timedelta(minutes=2)}
) as dag:
    
    def read_config_file():

        config = configparser.ConfigParser()
        config.read('/opt/airflow/dags/info.cfg')

        Variable.set(key = 'region', value = config.get('SUMMONER', 'region'))
        Variable.set(key = 'gameName', value = config.get('SUMMONER', 'gameName'))
        Variable.set(key = 'tagLine', value = config.get('SUMMONER', 'tagLine'))
        Variable.set(key = 'key', value = config.get('DEV_KEY', 'key'))


    def get_puuid(ti):

        region = Variable.get(key = 'region')
        gameName = Variable.get(key = 'gameName')
        tagLine = Variable.get(key = 'tagLine')
        key = Variable.get(key = 'key')

        url = f"https://{region}.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{gameName}/{tagLine}?api_key={key}"
        res = requests.get(url)
        ti.xcom_push(key = 'puuid', value = json.loads(res.text)["puuid"])


    def get_matchId_list(ti, start_date):

        region = Variable.get(key = 'region')
        key = Variable.get(key = 'key')
        puuid = ti.xcom_pull(key = 'puuid', task_ids = 'get_puuid')

        startTime = int(start_date)
        endTime = startTime + 604800
        
        url = f"https://{region}.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?startTime={startTime}&endTime={endTime}&type=ranked&start=0&count=100&api_key={key}"
        res = requests.get(url)
        ti.xcom_push(key = 'matchId_list', value  = json.loads(res.text))


    def get_matches_info(ti, date):
        
        region = Variable.get(key = 'region')
        gameName = Variable.get(key = 'gameName')
        key = Variable.get(key = 'key')
        matchId_list = ti.xcom_pull(key = 'matchId_list', task_ids = 'get_matchId_list')

        headers = [['matchId',
           'summoner_1', 
           'champion_1', 
           'summoner_2', 
           'champion_2',
           'summoner_3', 
           'champion_3',
           'summoner_4', 
           'champion_4',
           'summoner_5', 
           'champion_5',
           'summoner_6', 
           'champion_6',
           'summoner_7', 
           'champion_7',
           'summoner_8', 
           'champion_8',
           'summoner_9', 
           'champion_9',
           'summoner_10', 
           'champion_10',
           'matchWin']]
        total_data_list = [[]]

        if bool(matchId_list):
            for matchId in matchId_list:
                url = f"https://{region}.api.riotgames.com/lol/match/v5/matches/{matchId}?api_key={key}"
                res = requests.get(url)
                text = json.loads(res.text)["info"]["participants"]
                data_list = [matchId]

                for dict in text:
                    data_list.append(dict['summonerName'])
                    data_list.append(dict['championName'])

                    if dict['summonerName'] == gameName:
                        matchWin = dict['win']

                data_list.append(matchWin)
                total_data_list.append(data_list)

            data = pd.DataFrame(total_data_list, columns=headers)
            file_path = f'/opt/airflow/dags/matches/{date}.csv'
            data.drop(index=0).to_csv(file_path, index=False)
        return       


    task_1 = BashOperator(task_id = 'create_dir',
                          bash_command = 'mkdir -p "/opt/airflow/dags/matches" ')
    
    task_2 = PythonOperator(task_id = 'read_config',
                            python_callable = read_config_file)

    task_3 = PythonOperator(task_id = 'get_puuid',
                            python_callable = get_puuid)
 
    task_4 = PythonOperator(task_id = 'get_matchId_list', 
                            python_callable = get_matchId_list, 
                            op_kwargs = {'start_date': '{{ data_interval_start.strftime("%s") }}'})
    
    task_5 = PythonOperator(task_id = 'get_info',
                            python_callable = get_matches_info,
                            op_kwargs = {'date': '{{ data_interval_start.strftime("%Y-%m-%d") }}'})

    task_1 >> task_2 >> task_3 >> task_4 >> task_5