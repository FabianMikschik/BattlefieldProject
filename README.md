This project is a test project to play around with ariflow, dbtvault and gcp. 
I mainly used it to gain some experience with the tools as well as learn data vault modeling.

The data used is from https://api.gametools.network/docs. It is pulling collecting data about my go-to Battlefield 4 Server. The idea was to create a dashboard using Power BI, which shows information about the server as well as the statistics of current players.

To get started:
Create .env file: echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
Next: docker-compose up

I used a gcp service file for authentication. Todo: Use different method for authentication.
