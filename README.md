# Letterboxd Predictions

To run the service and baseline code, please set up environment variables __AWS_ACCESS_KEY_ID__ and __AWS_SECRET_ACCESS_KEY__  with the corresponding values for your __Amazon S3 Bucket__ access.
Bucket properties can be changed in `service/bucket_properties_.py`


## Scrapper
----

Scrapping movie information from [Letterboxd](https://letterboxd.com/) service.

Running script:

```
nohup python scrapper.py --start <start_page> --end <end_page> --filename <csv_filename> &
```

`start_page` and `end_page` correspond to pages of [popular movies](https://letterboxd.com/films/popular/).


## Service
---
Simple Flask API with `/predictions [POST]` endpoint.


### __v2__ (latest)
[Docker image](https://hub.docker.com/layers/andriankrav/letterboxd-predictions/2/images/sha256-94c991119ab4c205140c381b55f7fa64de9097764fac2ee4041cda5c323c9750?context=repo)

Body example:
```
{
    "Title": "Home Alone",
    "Year": 1990,
    "Views": 2337261,
    "Likes": 550460,
    "Minutes": 103,
    "Language": "English",
    "Genres": ["Comedy", "Family", "Adventure"],
    "Countries": ["USA"],
    "Directors": ["Chris Columbus"],
    "Cast": ["Macaulay Culkin", "Daniel Stern", "Joe Pesci", "Catherine O'Hara", "John Heard"]
}
```

Response example:
```
{
    "predicted_rating": 3.6
}
```

### __v1__
[Docker image](https://hub.docker.com/layers/andriankrav/letterboxd-predictions/2/images/sha256-94c991119ab4c205140c381b55f7fa64de9097764fac2ee4041cda5c323c9750?context=repo)

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

## AirFlow
---

__DAG__ for publishing/updating csv file on __Amazon S3 Bucket__.
Change local fields to your __S3 Bucket__ configurations and set up __AWS Connection__ in Airflow.

## Baseline
----

Fit simple __Linear Regression__ on data from __Amazon S3 Bucket__. Uploads model pickle to the same __S3 Bucket__.




