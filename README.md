# Deploying redshift and spectrum to analyze federal contract data

This project is intended to build a pipeline to enable ad hoc queries on 13 years of federal government contract data. This project is deployed on AWS platform and with following technologies:
* AWS S3
* Apache Spark
* AWS Redshift
* AWS Spectrum
* AWS EC2

Specturm was released at the same time I started this project, I would like explore the difference of this new service from Redshift so that I included it in my project.

## Federal Government Procurement data system

Federal Government Procurement data system archived 13 years government contract data. They are archived in XML files. Data on this system can identify who bought what, from which agency, for how much, when and where. 

Data was downloaded using django-admin. Just add a management/commands directory to the application shown below. Django will register a manage.py command for each Python module in that directory. 

```
DownloadFGPD\
	__init__.py
    models.py
    management/
        __init__.py
        commands/
            __init__.py
            _private.py
            download_xmls.py
    tests.py
    views.py
```

Data was downloaded by runing the command below

```
$python manage.py download_xmls
```

Then data was uploaded to AWS S3 with upload_dir_to_s3.py. Below is an example for the configure file:
```
[SectionOne]
sourceDir: SOURCEDIR
destDir: DESTDIR
Bucket: BUCKET_NAME
AWS_ACCESS_KEY_ID: aws_access_key
AWS_ACCESS_KEY_SECRET: aws_access_key_secret
MAX_SIZE: max_size
PART_SIZE: part_size
```

## Redshift

After extracting data from the federal contract system, I wrote the data to S3 using Spark data frames and then load it to Redshift.

<p align="center">
  <img src="/Picture1.png" width="900"/>
</p>

## Introduction to Spectrum

This architecture allows for the separation of resources used for storage and computation. The majority of data remains on S3 while Spectrum and Redshift nodes are used for computation. These two choices are much cost effective than running a single large redshift cluster.

<p align="center">
  <img src="/Spectrum.png" width="900"/>
</p>

This architecture allows for the separation of resources used for storage and computation. The majority of data remains on S3 while Spectrum and Redshift nodes are used for computation. These two choices are much cost effective than running a single large redshift cluster.
