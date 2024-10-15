# Ethiopian Medical DataWarehouse using YoloV5

A comprehensive data warehouse solution for Ethiopian medical business data scraped from Telegram channels, including data scraping, object detection with YOLO, and ETL/ELT processes.

# Screenshot that shows the FastApi Call

![alt text](image.png)

## Project Directory Structure

The repository is organized into the following directories:

`.github/workflows/`: Contains configurations for GitHub Actions, enabling continuous integration and automated testing.

`.vscode/`: Configuration files for the Visual Studio Code editor, optimizing the development environment.

`app`: Contains the implementation of the machine learning model API, allowing interaction with the model through RESTful endpoints.

`notebooks/`: Jupyter notebooks used for tasks such as data exploration, feature engineering, and preliminary modeling.

`scripts/`: Python scripts for data preprocessing, feature extraction, and the implementation of the credit scoring model.

`tests/`: Unit tests to ensure the correctness and robustness of the implemented model and data processing logic.



## Installation Instructions

To run the project locally, follow these steps:

1. Clone the Repository:
>>>>
    git clone https://github.com/epythonlab/bati-bank-credit-scoring.git`

    cd bati-bank-credit-scoring
>>>>

2. Set up the Virtual Environment:

Create a virtual environment to manage the project's dependencies:

**For Linux/MacOS**

>>>
    python3 -m venv .venv

    source .venv/bin/activate  
>>>

**For Windows:**

>>>
    python -m venv .venv
    .venv\Scripts\activate
>>>

3. Install Dependencies:

Install the required Python packages by running:
>>>
    pip install -r requirements.txt
>>>

## Contributing
 We welcome contributions to improve the project. Please follow the steps below to contribute:

- Fork the repository.
- Create a new branch for your feature or bugfix.
- Submit a pull request with a detailed explanation of your changes.
