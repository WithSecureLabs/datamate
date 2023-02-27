# Datamate (DM8)

- [About](#about)
- [Main feaures](#main-features)
    - [Data conversion](#data-conversion)
    - [Data visualization](#data-visualization)
- [Setup](#setup)
    - [Setup the data conversion part](#setup-the-data-conversion-part)
- [Usage](#usage)

## About

Datamate is a tool for security analysts to help performing data exfiltration analysis on large NetFlow/firewall data quickly and locally, without having to deploy complex infrastructure. 

## Main features

### Data conversion
Data conversion is Python script wrapped in a Docker image, which can be run as a Docker container.

In converts the raw data from JSON lines to Parqet.
### Data visualization

[Work In Progress - to be added later](#)

## Setup

Before going into the details regarding the setup designated for the main features, Docker Desktop should be installed and it can be found [here](https://www.docker.com/products/docker-desktop/)

### Setup the data conversion part

Here are the required steps in order to setup a Docker container which will convert the raw data from JSON lines format to Parquet format.

1. Open Docker Desktop as admin
2. From Docker Desktop open `Settings`
3. From Settings go to `Resources` > `File sharing`
4. Within File sharing add the local folder where the raw data is stored
5. Open a terminal and go to the folder where the repository is located
6. Build the Docker image using the Dockerfile

    ```docker build -t data_converter .```
    
    Parameters:
    - `-t data_converter` - name the docker image (in this case "data_converter")

7. Run the Docker image as Docker container

    ```docker run -it --rm -v <local_folder_path>:<container_folder_path> --memory="16g" --memory-swap="32g" --cpus="6.0" --env-file datamate.env data_converter /bin/bash```

    Parameters:
    - `--it` - will create an interactive bash shell
    - `--rm` - will detele the docker container once the session is closed
    - `-v <local_folder_path>:<container_folder_path>` - the volume mounted
    - `--memory="16g"` - memory limit
    - `--memory-swap="32g"` - swap limit equal to memory plus swap ("-1" for unlimited swap)
    - `--cpus="6.0"` - number of CPUs
    - `--env-file datamate.env` - read a file of environment variables
    - `data_converter` - specify the Docker image (it can have also a tag `name:tag`)
    - `/bin/bash` - starts the bash shell

After following all the above steps, the following prompt will be seen in the terminal: `root@515e7f248656:/converter#` (the number will be different as is randomly generated ateach run)

## Usage

Now that the Docker container is running, the actual conversion can be started. The only thing needed is to run from terminal `python dask_json_to_parquet.py`.

The following messages will be seen during the execution, highlighting the performed steps:

```24-02-2023, 09:03:50 Start converting
Create output folder if it doesn't exist
Initiate local cluster with 6 CPUs
Setup columns dtypes
Process data
Save to parquet
24-02-2023, 09:05:38 End converting
```

Depending on the amount of data, conversion can take some time and when is done, the Parquet file can be seen locally, in the `output` folder, created within `local_folder_path` provided.