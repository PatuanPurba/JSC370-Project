# JSC370-Project
JSC370 Project about Movie Analysis

## Setup
This section will take you through the procedure to take your development environment from zero to hero.
1. Install python from the official [website](https://www.python.org/downloads/).

    The project runs on python `3.13`.

2. Install [git](https://git-scm.com/).

3. Clone the repository.

    It is recommended that you use [Github Desktop](https://desktop.github.com/) to clone the project repository.

3. Install project dependencies

    From a terminal within the cloned repository, build virtual environment using following command:
    ```
    python -m venv .venv
    .venv/Scripts/activate
    ```

    Then install project dependency:
    ```
    python -m pip install requirements.txt
    ```

## Running

### Data Fetching
To fetch dataset, run `data_fetching.py`. User only need to run this once to build the dataset since the result of the dataset will be stored as csv for future running

### Main Analysis
Code for main analysis an report are stored in `main.qmd`. Since it's sequential (like .ipynb), it's important to keep the order of executing to ensure reproducibilty. 

### Getting Document
To build the document from the main analysis, run following command:
```
quarto render main.qmd
```