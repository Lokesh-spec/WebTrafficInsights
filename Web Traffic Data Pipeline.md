 
This project implements a traffic data pipeline that processes sensor data (mock), transforms it, and outputs results for further analysis or storage. The pipeline leverages Apache Beam for distributed processing and integrates with Google Cloud Dataflow, BigQuery, and other resources.
## Project Structure

### File Strucutre

├── README.md               # Project overview and documentation
├── data_generator          # Data generation logic for simulated traffic data
│   ├── __init__.py
│   └── generate_data.py    # Generates traffic data for testing and development
├── images                  # Folder containing images for reports or documentation
│   ├── BigQuery-Output.png
│   ├── DataFlow-Job.png
│   └── Pytest.png
├── inputs                  # Sample input files for the pipeline
│   ├── 13003e1a-1c91-4a9f-bab1-2a018a2d3598.out
│   └── 86e1def6-f372-4743-9788-8aca83c54f90.out
├── outputs                 # Folder where output files are saved (BigQuery, etc.)
├── pipelines               # Apache Beam pipeline scripts
│   ├── __init__.py
│   ├── minute_traffic.py   # Processes minute-level traffic data
│   └── user_traffic.py     # Processes user-level traffic data
├── resources               # Configuration and schema files
│   └── bigquery_schema.json
├── setup.py                # Setup script for the project
└── tests                   # Unit tests for pipeline components
    ├── __init__.py
    └── test_minute_traffic.py  # Tests for minute_traffic.py pipeline

## Setup Instructions

To set up this project, follow these steps:

1. **Clone the repository**:
``` bash
git clone https://github.com/your-repo/data-processing-pipeline.git
cd data-processing-pipeline
```

2. **Install required dependencies**: Install the Python dependencies using `pip`:
``` bash
pip install -r requirements.txt
```

3. **Set up Google Cloud (Optional)**: If you plan to use Google Cloud services (e.g., BigQuery, Dataflow):

- Ensure you have a Google Cloud account and create a new project.
- Set up authentication by downloading a service account key and setting the `GOOGLE_APPLICATION_CREDENTIALS` environment variable:
``` bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your-service-account-file.json"
```

## Usage

### Data Generation

To generate simulated traffic data, run the `generate_data.py` script in the `data_generator` directory. This will create input files for processing:

``` bash
python data_generator/generate_data.py -S batch -U 20 -E 10000 -L 300
```

### Running the Pipelines

You can run the Apache Beam pipelines using the following command:

``` bash
python pipelines/minute_traffic.py
```

Or for user-level traffic:

``` bash
python pipelines/user_traffic.py 
```

These scripts read traffic data, process it using Apache Beam, and output the results to a specified location (e.g., local storage or BigQuery).

### Configuration

You can configure the pipeline's behavior using a configuration file such as `dev_config.yaml` (if present in your project). This file defines settings like the runner type (DirectRunner, DataflowRunner), input/output paths, and other parameters.

## Testing

This project uses [Pytest](https://docs.pytest.org/en/stable/) for testing. To run the tests, execute the following command:

``` bash
pytest tests/
```

The tests will validate the functionality of the pipeline components. For example, `test_minute_traffic.py` contains tests for the `minute_traffic.py` pipeline.

## Resources

- **BigQuery Schema**: The `resources/bigquery_schema.json` file contains the schema used to load data into BigQuery.

### Example Image Outputs

- **BigQuery Output**:
- **DataFlow Job**:
- **Pytest Output**:

## License

This project is licensed under the MIT License - see the LICENSE file for details. 