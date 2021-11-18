import uvicorn
from fastapi import FastAPI
from predict import predict_churn_by_player_id

# App creation and model loading
app = FastAPI()

# TODO try make this request as async
@app.post('/predict')
def predict(player_id):
    """
    :param player_id: input string player id for predict
    :return: predicted churn player probability
    """
    probability_player_churn = predict_churn_by_player_id(player_id)
    return probability_player_churn


if __name__ == '__main__':
    # Run server using given host and port
    uvicorn.run(app, host='127.0.0.1', port=8080)
