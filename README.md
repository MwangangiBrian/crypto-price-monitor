# Monitoring Crypto Data in Real Time

## Repository Structure

```shell
crypto-tracker/
├── .idea/
├── crypto_stream.py
├── dags/
│   ├── get_data.py
│   └── kafka_stream.py
├── docker-compose.yml
├── README.md
├── script/
│   └── entrypoint.sh
├── requirements.txt
└── .venv/
    ├── Lib/
    └── Scripts/
```

## Getting Started

1- Clone the repository:

```
git clone https://github.com/nits302/Crypto-Real-Time-Data-Streaming.git
```

2- Navigate to the project directory

```
cd streaming_realtime_data
```

3- Set Up a Virtual Environment in Python

```
pip3 install virtualenv
python -m venv venv
source venv/bin/activate
```

4- Install the needed packages and libraries:

```
pip3 install -r ./requirements.txt
```

5- Install Docker, Docker compose:

```
sudo ./installdocker.sh
docker --version
docker compose version
```

6- Build docker:

```
docker compose up -d
```

7- Run step by step files:

```
python crypto-streaming.py
```

8- Access to airflow UI to monitor streaming process:

```
localhost:8080 account: admin, password: admin
```

9- Access to control center to monitor Topic health, Procuder and consumer performance, and Cluster health:

```
localhost:9021
```

10- Visualize in `Grafana`:

```
http://localhost:3000
```
