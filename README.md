# Letterboxd Predictions

To run the service and baseline code, please update `service/properties/local_properties.py` with the corresponding values for your __Amazon S3 Bucket__.


And create `service/properties/secret.py` with the following constants:

```
AWS_ACCESS_KEY_ID = '<AWS_ACCESS_KEY_ID>'
AWS_SECRET_ACCESS_KEY = '<AWS_SECRET_ACCESS_KEY>'
```

### Scrapper
----

Scrapping movie information from [Letterboxd](https://letterboxd.com/) service.

Running script:

```
nohup python scrapper.py --start <start_page> --end <end_page> --filename <csv_filename> &
```

`start_page` and `end_page` correspond to pages of [popular movies](https://letterboxd.com/films/popular/).


### Service
---

Simple Flask API with `/predictions [POST]` endpoint.

Body example:
```
{
    "Title": "Home Alone",
    "Year": 1990,
    "Views": 2337261,
    "Likes": 550460,
    "Minutes": 103
}
```

Response example:
```
{
    "predicted_rating": 3.6
}
```

Can be used as [Docker image](https://hub.docker.com/repository/docker/andriankrav/letterboxd-predictions/general).

### AirFlow
---

__DAG__ for publishing/updating csv file on __Amazon S3 Bucket__.
Change local fields to your __S3 Bucket__ configurations and set up __AWS Connection__ in Airflow.

### Baseline
----

Fit simple __Linear Regression__ on data from __Amazon S3 Bucket__. Uploads model pickle to the same __S3 Bucket__.




