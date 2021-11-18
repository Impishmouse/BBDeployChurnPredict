from pydantic import BaseModel
import joblib
import player
from player import PlayerLost

model = joblib.load("churn_f24_v4.sav")


class PredictResult(BaseModel):
    """
    Input features validation for the ML model
    """
    probability: float
    status: str


def predict_churn_by_player_id(player_id) -> PredictResult:
    result = PredictResult(probability = 0, status = "Error")

    try:
        data = player.get_player_full_data(player_id)
        probability_player_churn = predict_player_churn(data)
        result.status = 'success'
        result.probability = probability_player_churn
    except PlayerLost:
        result.status = "Error; cant find player"
        result.probability = 0

    return result


def predict_player_churn(data):
    probability_player_churn = model.predict_proba(data)
    return probability_player_churn


if __name__ == '__main__':
    predict_churn_by_player_id('dfsdf')