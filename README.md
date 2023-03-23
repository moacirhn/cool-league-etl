# cool-league-etl
An ETL of simple match data of a single summoner from the game League of Legends. This project was orcehtrated using Apache Airflow in Docker.

The code reads from an omitted .cfg file conteining sensitive information such as the Riot API access key and the summoner in-game nickname.

At the moment, the code can only do few requests to the API due to it using a development API key witch has the lowest rate limit of request known to Riot, on top of the need to daily reset it. This may cause the run to fail if you run it to many times. For exemple, trying to retroactively get data from past weeks(the code was intended to do it since 2021 july 13th. It is the limit specified in the documentation) will most definitely result in too many dag runs failing due to the lack do data returned by the API.