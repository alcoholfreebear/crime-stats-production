# crime-stats-production
Source codes for web app: 

https://crimemap.se

Folder structure: 
```
.
├── LICENSE
├── cloudbuild.yaml
├── README.md
├── backapp
│   ├── app.yaml
│   ├── config.py
│   ├── cron.yaml
│   ├── main.py
│   ├── migration.py
│   ├── operations.py
│   └── requirements.txt
└── frontapp
    ├── Dockerfile
    ├── app_data.py
    ├── app_funcs.py
    ├── main.py
    └── requirements.txt

```

# Backend app

This app is responsible for the backend data pipeline of the app. 

## Codes
Source codes in `backapp` folder. 

Main logics in `backapp/main.py` (the flask app) and `backapp/operations.py`:
1. Request high level crime report json data from https://polisen.se/api/events. 
2. Parse the json data and then scrape the webpage in "url" field for crime report details. For example information in this [web page](https://polisen.se/aktuellt/handelser/2022/augusti/2/02-augusti-0912-trafikolycka-hogsby/).
3. Grab street and city names from the detail data and request the latitude and longitude using mapbox API.
4. Translate the details data into English.
5. Combine all the information into a dataframe and push to Google Cloud BigQuery and google cloud storage bucket as parquet file. 

## Deployment

This app is deployed using app engine and scheduled by cron jobs. 

```
# deploy app.yaml
gcloud app deploy app.yaml --version=v1

# deploy cron.yaml
gcloud app deploy cron.yaml
```

# Frontend app
Interactive visualization in the webapp: https://crimemap.se .

## Codes
Source codes in `frontapp` folder. 

Main logic: 
1. Read data from Google Cloud storage bucket. See `frontapp/app_data.py`. 
2. Get map data form mapbox API and other data processing steps. See `frontapp/app_func.py`.
3. Visualize data interactively using dash and plotly in `frontapp/main.py`.   

## Deployment
This app is deployed as a cloud run app with cloud build. `./cloudbuild.yaml` contains the following build steps:
1. Install dependency.
2. Build docker image according to `.frontapp/Dockerfile`. 
3. Push the image to GCP artifact registry.
4. Deploy the image to cloud run.

Cloud build triggers are set for the following events:
* Daily at 8:20 and 20:20. 
* On push to master branch of this github repository.

### Deployment Resources
1. [Deploy GCP cloud run apps using Cloud build](https://cloud.google.com/build/docs/deploying-builds/deploy-cloud-run)

2. [GCP cloud build triggers](https://cloud.google.com/build/docs/automating-builds/create-manage-triggers#gcloud) 









