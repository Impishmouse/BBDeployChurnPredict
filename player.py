import datetime
from db_connector import get_client
from pandas import DataFrame
import pymongo


# define Python user-defined exceptions
class Error(Exception):
    """Base class for other exceptions"""
    pass


class PlayerLost(Error):
    """Raised when the input value is too small"""
    pass


def get_players_by_dates(collection, from_date, to_date, max_level=8):
    player_cursor = collection.find({
        "_created_at": {
            "$gte": from_date,
            "$lt": to_date,
        },
        "maxDoneLevel": {"$gte": max_level},
        "isTest": {"$not": {"$eq": True}},
    }).sort("_updated_at", direction=pymongo.DESCENDING)

    list_players = list(player_cursor)
    return DataFrame(list_players)


def get_player_by_id(collection, player_id):
    player_cursor = collection.find({
        "_id": player_id,
    }).sort("_updated_at", direction=pymongo.DESCENDING)

    list_players = list(player_cursor)
    players_df = DataFrame(list_players)
    if players_df.empty:
        raise PlayerLost

    player_id = players_df['_id']
    player_id = player_id.map(lambda player: 'Player$' + player)
    players_df['_id'] = player_id

    return players_df


def drop_bb_unnecessary_data(bb_player_data):
    bb_player_data.drop("fbId", axis=1, inplace=True, errors='ignore')
    bb_player_data.drop("fbActive", axis=1, inplace=True, errors='ignore')
    bb_player_data.drop("fbReward", axis=1, inplace=True, errors='ignore')
    bb_player_data.drop("GDPR", axis=1, inplace=True, errors='ignore')
    bb_player_data.drop("version", axis=1, inplace=True, errors='ignore')
    bb_player_data.drop("isTest", axis=1, inplace=True, errors='ignore')
    bb_player_data.drop("daysActive", axis=1, inplace=True, errors='ignore')
    bb_player_data.drop("smdbDate", axis=1, inplace=True, errors='ignore')
    bb_player_data.drop("appleUID", axis=1, inplace=True, errors='ignore')
    bb_player_data.drop("appleActive", axis=1, inplace=True, errors='ignore')
    bb_player_data.drop("allowedErrorsCollected", axis=1, inplace=True, errors='ignore')
    return bb_player_data


def get_failures_by_player(player_id, db):
    failures_collection = db['Failures']
    failures_cursor = failures_collection.find({
        "_p_playerId": player_id,
    }).sort("_updated_at", direction=pymongo.DESCENDING)

    list_failures = list(failures_cursor)

    if len(list_failures) == 0:
        return None

    failures_df = DataFrame(list_failures)

    return failures_df


def get_victories_by_player(player_id, db):
    victories_collection = db['Victories']
    victories_cursor = victories_collection.find({
        "_p_playerId": player_id,
    }).sort("_updated_at", direction=pymongo.DESCENDING)

    list_victories = list(victories_cursor)

    if len(list_victories) == 0:
        return None

    victories_df = DataFrame(list_victories)

    return victories_df


def get_dialogs_by_player(player_id, db):
    dialog_showed_collection = db['DialogsShowing']
    dialogs_cursor = dialog_showed_collection.find({
        "_p_playerId": player_id,
        "$or": [
            {"dialogType": {"$eq": "RewarderADS_predefeat_button"}},
            {"dialogType": {"$eq": "RewarderADS_map_icon"}}
        ]
    }).sort("_updated_at", direction=pymongo.DESCENDING)

    list_dialogs = list(dialogs_cursor)
    dialogs_df = DataFrame(list_dialogs)

    return dialogs_df


def get_ads_data(player_id, dialogs_df):
    if dialogs_df.empty:
        ret = {'tAd': 0,
               'mapAd': 0,
               'pDefAd': 0, }
        return ret

    player_ads = dialogs_df[(dialogs_df['_p_playerId'] == player_id) & (dialogs_df['dialogName'] == "yes_reward")]
    total_ads = player_ads.shape[0]
    ret = {'tAd': total_ads}
    map_ads = player_ads[player_ads['dialogType'] == 'RewarderADS_map_icon'].shape[0]
    pre_def_ads = player_ads[player_ads['dialogType'] == 'RewarderADS_predefeat_button'].shape[0]
    ret['mapAd'] = map_ads
    ret['pDefAd'] = pre_def_ads

    return ret


def get_day_fail_info(day_fail_data, day_pref):
    if day_fail_data is None:
        return {day_pref + "_fc": 0}

    count = day_fail_data.shape[0]
    d = {day_pref + "_fc": count}
    return d


def get_data_from_day(some_data, start_date, day_range):
    if some_data is None:
        return None

    end_date = start_date + datetime.timedelta(days=day_range)
    rule = ((some_data['_created_at'] >= start_date) & (some_data['_created_at'] < end_date))
    return some_data[rule]


def get_data_before_day(some_data, end_date, days_before):
    if some_data is None:
        return None

    from_day = end_date - datetime.timedelta(days=days_before)
    rule = ((some_data['_created_at'] >= from_day) & (some_data['_created_at'] < end_date))
    return some_data[rule]


def failures_data(player_id, fails_df, update_date, create_date):
    if fails_df is None:
        total_fails = 0
        fails = None
    else:
        fails = fails_df[(fails_df['_p_playerId'] == player_id)]
        total_fails = fails.shape[0]

    ret = {"tFail": total_fails, }
    date_to = update_date
    days_prefixes = ['dU', 'dU-1']
    for i in range(2):
        fails_at_range = get_data_before_day(fails, date_to, 1)
        res_falis = get_day_fail_info(fails_at_range, days_prefixes[i])
        ret = {**ret, **res_falis}
        date_to = date_to - datetime.timedelta(days=1)

    date_from = create_date
    # Get failures count from creation day
    for i in range(1, 9):
        if i % 2 != 0:
            if date_from < update_date:
                data_at_range = get_data_from_day(fails, date_from, 1)
                res_fails = get_day_fail_info(data_at_range, f"d{i}")
                ret = {**ret, **res_fails}
                date_from = date_from + datetime.timedelta(days=i)

    return ret


def get_day_victory_info(day_victory_data, day_prefix):
    if day_victory_data is None:
        return {day_prefix + "_vc": 0}

    count = day_victory_data.shape[0]
    d = {day_prefix + "_vc": count}
    return d


def victories_data(player_id, victories_df, update_date, create_date):
    if victories_df is None:
        total_victories = 0
        vict = None
    else:
        vict = victories_df[(victories_df['_p_playerId'] == player_id)]
        total_victories = vict.shape[0]

    date_to = update_date
    ret = {"tVic": total_victories}

    days_prefixes = ['dU', 'dU-1']
    for i in range(2):
        if create_date < date_to:
            vict_at_range = get_data_before_day(vict, date_to, 1)
            res_vict = get_day_victory_info(vict_at_range, days_prefixes[i])
            ret = {**ret, **res_vict}
            date_to = date_to - datetime.timedelta(days=1)

    date_from = create_date
    # Get victories count from day
    for i in range(1, 9):
        if i % 2 != 0:
            if date_from < update_date:
                vict_at_range = get_data_from_day(vict, date_from, 1)
                res_vict = get_day_victory_info(vict_at_range, f"d{i}")
                ret = {**ret, **res_vict}
                date_from = date_from + datetime.timedelta(days=i)

    return ret


def fill_nan_data(player_df):
    features = ['initStep', 'sessionsCount', 'starsCount', 'smdbIndex', 'competitionLeague',
                'dU-1_vc', 'd3_vc', 'd5_vc', 'd7_vc', 'd3_fc', 'd5_fc', 'd7_fc']
    for key in features:
        if player_df.columns.str.contains(key).any():
            player_df[key].fillna(0, inplace=True)
    return player_df


def map_to_bool(player_df):
    player_df['iOS'] = player_df['iOS'].map({True: 1, False: 0})
    player_df['isPaid'] = player_df['isPaid'].map({True: 1, False: 0})
    player_df['android'] = player_df['android'].map({True: 1, False: 0})
    return player_df


def group_max_done_level(players_df):
    players_df['maxLevel_g0'] = players_df['maxDoneLevel'].map(lambda val: (val // 10) * 10 if val >= 50 else val)
    return players_df


def clear_player_data(players_df):
    # drop features witch have low importance  v1
    players_df = players_df.drop("_id", axis=1, errors="ignore")
    players_df = players_df.drop("_created_at", axis=1, errors="ignore")
    players_df = players_df.drop("_updated_at", axis=1, errors="ignore")
    players_df = players_df.drop("CurrentDevice", axis=1, errors="ignore")
    players_df = players_df.drop("isPaid", axis=1, errors="ignore")
    players_df = players_df.drop("realDiamond", axis=1, errors="ignore")
    players_df = players_df.drop("initStep", axis=1, errors="ignore")
    players_df = players_df.drop("tCont", axis=1, errors="ignore")
    players_df = players_df.drop("tFCont", axis=1, errors="ignore")
    players_df = players_df.drop("maxDoneLevel", axis=1, errors="ignore")
    players_df = players_df.drop("competitionLeague", axis=1, errors="ignore")
    players_df = players_df.drop("resetProgress", axis=1, errors="ignore")

    return players_df


# player_id = 'F9q0JK2Jeu'
def get_player_full_data(player_id):
    start_dt = datetime.datetime.now()

    client = get_client()
    if client is None:
        print("Unable connect to db")
        return

    print("Success connect to db")

    db = client['best-birds']
    player_collection = db['Player']

    local_dt = datetime.datetime.now()
    player_df = drop_bb_unnecessary_data(get_player_by_id(player_collection, player_id))
    print(f"Success get player; time: {(datetime.datetime.now() - local_dt)}")

    player_pointer = player_df['_id'][0]
    local_dt = datetime.datetime.now()
    dialogs_df = get_dialogs_by_player(player_pointer, db)
    print(f"Success get AD's dialogs data for player; time: {(datetime.datetime.now() - local_dt)}")

    local_dt = datetime.datetime.now()
    failures_df = get_failures_by_player(player_pointer, db)
    print(f"Success get failures data for player; time: {(datetime.datetime.now() - local_dt)}")

    local_dt = datetime.datetime.now()
    victories_df = get_victories_by_player(player_pointer, db)
    print(f"Success get victories data for player; time: {(datetime.datetime.now() - local_dt)}")

    one_player_update = player_df['_updated_at'].iloc[0]
    one_player_create_day = player_df['_created_at'].iloc[0]
    delta_days = one_player_create_day - one_player_update

    player_df.loc[0, 'days'] = abs(delta_days.days)
    # get features from Victories
    res_data = victories_data(player_pointer, victories_df, one_player_update, one_player_create_day)
    for feature in res_data:
        player_df.loc[0, feature] = res_data[feature]
    # get features from Failures
    res_data = failures_data(player_pointer, failures_df, one_player_update, one_player_create_day)
    for feature in res_data:
        player_df.loc[0, feature] = res_data[feature]
    # add info about watches Ad's
    res_data = get_ads_data(player_pointer, dialogs_df)
    for feature in res_data:
        player_df.loc[0, feature] = res_data[feature]

    # final data for player prepare
    player_df = map_to_bool(player_df)
    player_df = fill_nan_data(player_df)
    player_df = group_max_done_level(player_df)
    player_df = clear_player_data(player_df)
    print(f"Success data prepared; Total time: {(datetime.datetime.now() - start_dt)}")
    return player_df


if __name__ == "__main__":
    get_player_full_data("4QvW1Cfn08")
# else:
#    print("File one executed when imported")
