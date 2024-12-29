import pandas as pd
import sklearn

#Import du dataset
file_path = "Combined_Flights_2018.parquet"
data = pd.read_parquet(file_path)

#Enlève les colonnes inutiles pour le modèle
data = data.drop(columns=['CRSDepTime','DepTime','ArrTime','CRSElapsedTime','ActualElapsedTime',
    'Quarter', 'Flight_Number_Marketing_Airline', 'Operating_Airline',
    'DOT_ID_Operating_Airline', 'IATA_Code_Operating_Airline',
    'Marketing_Airline_Network','Operated_or_Branded_Code_Share_Partners',
    'DOT_ID_Marketing_Airline','IATA_Code_Marketing_Airline',
    'Tail_Number','Flight_Number_Operating_Airline', 
    'OriginAirportID', 'OriginAirportSeqID', 'OriginCityMarketID', 'OriginCityName',
    'OriginState', 'OriginStateFips', 'OriginStateName', 'OriginWac',
    'DestAirportID', 'DestAirportSeqID', 'DestCityMarketID', 'DestCityName',
    'DestState', 'DestStateFips', 'DestStateName', 'DestWac', 'DepDel15',
    'DepartureDelayGroups', 'DepTimeBlk', 'TaxiOut', 'WheelsOff',
    'WheelsOn', 'TaxiIn', 'CRSArrTime', 'ArrDel15',
    'ArrivalDelayGroups', 'ArrTimeBlk', 'DistanceGroup'])

#print(data.head())

#print(data.columns)

from sklearn.preprocessing import LabelEncoder

# Créer un encodeur
encoder = LabelEncoder()

# Encoder la colonne Airline, Origin et Dest
data['Airline_encoded'] = encoder.fit_transform(data['Airline'])
data['Origin_encoded'] = encoder.fit_transform(data['Origin'])
data['Dest_encoded'] = encoder.fit_transform(data['Dest'])

# Remplissage de 0 sur les NaN pour éviter les erreurs
data = data.fillna(0)

#Séparation des caractéristiques pour les retards
features_delay = data[['Year', 'Month', 'DayofMonth','Airline_encoded','Origin_encoded','Dest_encoded','Distance','DayOfWeek']] # Colonnes de caractéristiques
target_delay = data[['DepDelay','ArrDelay']] # Colonne cible

X_delay = features_delay.values
y_delay = target_delay.values

#Séparation des caractéristiques pour les déviations
features_divert = data[['Year', 'Month', 'DayofMonth','Airline_encoded','Origin_encoded','Dest_encoded','Distance','DayOfWeek']] # Colonnes de caractéristiques
target_divert = data[['Diverted','DivAirportLandings']] # Colonne cible

X_divert = features_divert.values
y_divert = target_divert.values

#Séparation des caractéristiques pour les annulations
features_cancel = data[['Year', 'Month', 'DayofMonth','Airline_encoded','Origin_encoded','Dest_encoded','Distance','DayOfWeek']] # Colonnes de caractéristiques
target_cancel = data['Cancelled'] # Colonne cible

X_cancel = features_cancel.values
y_cancel = target_cancel.values

#Préparation des données pour l'entrainement (retard)
from sklearn.model_selection import train_test_split

X_train, X_test, y_train, y_test = train_test_split(X_delay, y_delay, test_size=0.2, random_state=42)

from sklearn.ensemble import RandomForestClassifier

# Créer le modèle
model = RandomForestClassifier(n_estimators=100, random_state=42)

# Entraîner le modèle avec les données d'entraînement
model.fit(X_train, y_train)

from sklearn.metrics import accuracy_score, classification_report

# Prédire sur les données de test
y_pred = model.predict(X_test)

# Calculer la précision
accuracy = accuracy_score(y_test, y_pred)
print(f"Accuracy: {accuracy * 100:.2f}%")

# Afficher un rapport détaillé de la performance du modèle
print(classification_report(y_test, y_pred))

from sklearn.metrics import mean_squared_error

# Calculer l'erreur quadratique moyenne pour la régression
mse = mean_squared_error(y_test, y_pred)
print(f"MSE: {mse:.2f}")
