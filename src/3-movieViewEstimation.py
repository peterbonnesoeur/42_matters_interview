
import pandas as pd
from sklearn.model_selection import cross_val_score
from sklearn.tree import DecisionTreeRegressor
from sklearn.linear_model import LinearRegression

import warnings
warnings.filterwarnings('ignore')

INPUT_FILE = "data/top-250-movie-ratings.csv"
OUTPUT_FILE = "output/moviesViewEstimations.csv"

RANDOM_STATE = 100

data = pd.read_csv(INPUT_FILE)

#We can see that Raiting count is a string, we will to convert it as a int for the rest of the exercise.
if data['Rating Count'].dtype == 'object':
    data['Rating Count'] = data['Rating Count'].str.replace('[^\d.]', '').astype(int)


# removing the col Unnamed: 0 as it is not needed
data.drop(columns=['Unnamed: 0'], axis = 1)

# Adding the limited view data

film_names = ["Forrest Gump", "The Usual Suspects", "Rear Window", "North by Northwest", "The Secret in Their Eyes", "Spotlight"]
film_views = [10000000, 7500000, 6000000, 4000000, 3000000, 1000000]

for i in range(len(film_names)):
    id = data['Title'] == film_names[i]
    data.loc[id,"views"] = film_views[i]

# choose the input and outputs of the model
input_features = ['Year', 'Rating', 'Rating Count']
output_feature = ['views']

X = data[data['Title'].isin(film_names)][input_features]

y = data[data['Title'].isin(film_names)][output_feature]

linear_model = LinearRegression().fit(X, y)

regr = DecisionTreeRegressor(max_depth = 3, random_state = RANDOM_STATE)

regr_model = regr.fit(X, y)

linear_model = LinearRegression().fit(X, y)
data["predicted_views"] = 3/4 * linear_model.predict(data[input_features])
data["predicted_views"] +=  1/4 * regr_model.predict(data[input_features])

# Saving the results
data.to_csv(OUTPUT_FILE)


