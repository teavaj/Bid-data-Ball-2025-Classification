Note: this is a prrof-of-concept project, focusing more on the things, I've learned over the years and not an actual entry for the bellow mentioned competition.

# Classification Project

This project involves
- An explorotary data analysis phase
- Gathering and cleaning data from CSV tables from the NFL Big Data Bowl 2025 Kaggle competition (https://www.kaggle.com/competitions/nfl-big-data-bowl-2025/overview)
- Further play-by-play data from https://nflsavant.com/about.php
- Creating and calculating extra metrics
- Joining every table together and writing it to a .parquet file
- Seting up a pipeline for preprocessing the data for modell training-testing
- Building and evaluating a LogisticRegression modell from sklearn on an NFL dataset using Python
- Generating synthetic data for validation and evaulation.

## Project Structure

- `notebooks/classification_PlayType.ipynb`: Jupyter notebook containing the main code for model training, and evaluation.
- `classification_pipeline.py`: Python script for data preprocessing pipeline.
- `classification_data.py`: Joining every datasource to a single file
- `smalldata/classification.parquet`: Dataset used for classification.

## Requirements

- Python 3.9+
- `requirements.yml`

## Data Preprocessing

The data is preprocessed using the cat_pipiline function from the classification_pipeline module. The dataset is read from a Parquet file and prepared for model training.
- `NFL Dataset Description.md`: official NFL data
- `pbp_2022.py`: play-by-play data from https://nflsavant.com/about.php for the 2022 season (based on the NFL dataset)
- `anya.py`: ANY/A (Adjusted Net Yards per Attempt). Calculated for every play per game
- `weather_data.py`: Weather data for every game with inforamtion about temperature, wind, rainfall.
- Stadium data: 0 - open, 1 - closed
- `player_targets.py`: Calculate cumulative sum of rushing/receiving targets for each player in each game

## Model Training and Evaluation

### The following models are trained and evaluated using cross-validation:
- RandomForestClassifier
- SVC
- LogisticRegression
- KNN
- AdaBoostClassifier
- XGBClassifier

The results are stored in a dictionary with the structure {col: {model: mean([v])}}, where col is the target variable, model is the model name, and v is the cross-validation score.

### Additional notes
- Because the tree models were overfitting with the default parameters, they were ruled out from further analysis.
- `notebooks/classification_PlayType_clusters.ipynb` contains an additional feature, created with KMeans, but the model's performance didn't improve.
- `notebooks/classification_PlayType_OneHot.ipynb` contains a new training set with one-hot encoded categorical features. This ensured that the LogisticRegression modell would converge. The model performed better out of the box, but for a same good performace, it was necceseary to take the same finetuning steps. 


## Visualization

The project includes visualizations for correlation matrices, confusion matrices and feature correlations using Seaborn and Yellowbrick.
