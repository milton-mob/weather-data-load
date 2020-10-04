## Getting Started Running The Project
In the command line go to the project's home directory(weather_data_load).  This project uses python 3.7:
```bash
# Create a python virtual environment:
python3 -m venv .venv

# Activate the virtual env
source ./.venv/bin/activate
```

You will need to have a few python modules installed before you can run this project:
```bash
# Install required packages
pip install -r requirements.txt
```

## Running the tests
To check if the installation steps above ran fine:

```bash
pytest
```

## Running the project

```bash
python ./main.py
```

The purpose of this project is to:
- Load weather data from csv files
- The loaded data is processed to answer the following questions.  The answer to this questions is printed to the screen:
    - Which date and time had the hottest temperature?
    - What was the hotest temperature on that day? 
    - In which region was the hottest temperature?
    - Which date was the overall hottest day?
    - What was the overall temperature on that day? 
    - In which region was the overall hottest day?
