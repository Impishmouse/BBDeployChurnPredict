from predict import predict_churn_by_player_id

if __name__ == '__main__':

    # player -  CiFeORDTy1 - predict - 0.171788 (old) churn: 0.11205339
    # player -  Xmm3k78OGN - predict - 0.138887 (old) churn: 0.19579174305852626
    # id: 4QvW1Cfn08 - 0.736305 churn:0.4759092807941694
    # me
    # player -  F9q0JK2Jeu - predict - 0.19689083 (old) churn: 0.19689082600450125
    # after not playing Probability that player churn:0.13645080216703287
    # Probability that player churn:[[0.8635492 0.1364508]]

    # id:  2YJO24vzfN - pred - 0.609537681224889
    # Player$Vde4NfW9Sb	0.780923 - 0.6691001781843475
    # Player$PhxHzCG48F	0.962267 - churn:0.7833898920439313
    player_id = "F9q0JK2Jeu"
    probability_player_churn = predict_churn_by_player_id(player_id)
    print(probability_player_churn)

