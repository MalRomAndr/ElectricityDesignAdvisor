import joblib
from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import LabelEncoder

encoder_struct = LabelEncoder()
encoder_type = LabelEncoder()
scaler = MinMaxScaler()

features = joblib.load('df.pkl').drop(columns=['Id_x', 'Name'], axis=1)
features['StructureId'] = encoder_struct.fit_transform(features['StructureId'])
features['TypeId'] = encoder_type.fit_transform(features['TypeId'])
features = scaler.fit_transform(features)
model = NearestNeighbors(n_neighbors=100, metric='cosine')
model.fit(features)


joblib.dump(encoder_struct, 'encoder_struct.pkl')
joblib.dump(encoder_type, 'encoder_type.pkl')
joblib.dump(scaler, 'scaler.pkl')
joblib.dump(features, 'features.pkl')
joblib.dump(model, 'model.pkl')
