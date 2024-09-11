# Mugshot Coffee

## Elevator pitch

Mugshot Coffee is developing a sophisticated ETL (Extract, Transform, Load) pipeline to efficiently process and analyse transaction data. Our cloud-based solution leverages Amazon Web Services (AWS) to provide real-time insights for shop owners to enhance decision-making and business performance..

![Grafana visualisations](https://github.com/agi-chan/mugshot-coffee/blob/main/visualisations.png?raw=true)
![Grafana monitoring](https://github.com/agi-chan/mugshot-coffee/blob/main/monitoring.png?raw=true)

## Team members

- Liam D <a href="https://www.github.com/Liam-Dignum" target="_blank">@Liam-Dignum</a>
- Alina A <a href="https://www.github.com/alina951" target="_blank">@alina951</a>
- Colvin S <a href="https://www.github.com/csrs42" target="_blank">@csrs42</a>
- Alex H <a href="https://www.github.com/agi-chan" target="_blank">@agi-chan</a>
  
## Project overview

Our ETL pipeline processes data through the following stages:

1. Extract: Retrieve data from CSV files which are uploaded to an S3 bucket
2. Transform: Normalise and clean the data in ExtractTransform Lambda.
3. Load: Data is sent to Load Lambda, which is then sent to a Redshift database
4. Visualise: Present insights through Grafana dashboards

## Architecture

![architecture](https://github.com/agi-chan/mugshot-cafe/blob/main/graph%20crop.png?raw=true)

![AWS architecture](https://github.com/agi-chan/mugshot-cafe/blob/main/aws.png?raw=true)

Our solution utilizes the following AWS components:

- S3: For storing raw CSV files
- Lambda: For ETL processing
- Redshift: As our data warehouse
- Cloudformation: To initialise infrastructure (bucket and Lambda) from .YAML template
- EC2

## Repository structure

```
Mugshot-Coffee/
├── .github/
│   └── workflows/
│       └── action.yml
├── Data/
│   ├── leeds_09-05-2023_09-00-00.csv
│   └── test_data.csv
├── lambda-layer/
│   └── lambda-layer.zip
├── .env
├── mugshot_lambda.py
├── load_lambda.py
├── extracttransform_lambda.py
├── README.md
├── connect_db.py
├── database.sql
├── docker-compose.yml
├── lambdatemplate.yaml
├── test_unit_tests.py
```
### Folder/Files Description

 - .github/: GitHub workflows and actions
 - .vscode/: Visual Studio Code settings.
 - Data/: Contains raw and sample data files
 - lambda-layer/: Python libraries for Lambda functions.
 - .env: Environment variables configuration.
 - Main.py: Main entry point of the application.
 - config.py: Configuration settings.
 - connect.py: Database connection utilities.
 - database.ini: Database configuration file.
 - database.sql: SQL scripts for database setup.
 - docker-compose.yml: Docker Compose configuration.
 - lambda.py: Lambda function code.
 - lambdatemplate.yaml: AWS CloudFormation template for Lambda functions.
 - test_unit_tests.py: Unit tests for the application.
 - transactions_data.csv: Sample transaction data.

## Getting Started

### Prerequisites

- Docker
- AWS CLI
- Python 3.12+

### Local setup

1. Clone the repository
   - git clone https://github.com/agi-chan/mugshot-coffee.git
   - cd Mugshot-Coffee
2. Set up envirnment variables
3. Install dependecies.
   - pip install -r requirements.txt
4. Run docker compose
   - docker-compose up
5. Deploy infrastructure (use aws cloudformation )
   - aws cloudformation create-stack --stack-name mugshot-coffee --template-body file://lambdatemplate.yaml
6. Run the application
   - python Main.py

### License
This project is licensed under the MIT License. See the LICENSE file for details.
