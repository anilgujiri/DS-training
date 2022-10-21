# importing necessary libraries and functions
import numpy as np
from flask import Flask, request, jsonify, render_template
import pickle
from joblib import dump, load

app = Flask(__name__) #Initialize the flask App
model = pickle.load(open(r'C:/Anil/Projects/CricketScorePredictor-master/model.pkl', 'rb')) # loading the trained model
sc = load(r'C:/Anil/Projects/CricketScorePredictor-master/std_scaler.bin') 

@app.route('/') # Homepage
def home():
    return render_template('index.html')

@app.route('/predict',methods=['POST'])
def predict():
    '''
    For rendering results on HTML GUI
    '''
    
    # retrieving values from form
    init_features = [int(x) for x in request.form.values()]
    
    #final_features = [np.array(init_features)]

    prediction =  model.predict(sc.transform(np.array(init_features)))


    return render_template('index.html', prediction_text='Predicted Score: {}'.format(prediction)) # rendering the predicted result

if __name__ == "__main__":
    app.run(debug=True)