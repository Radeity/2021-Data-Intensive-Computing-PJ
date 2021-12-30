# hdfs dfs -put /home/arshgoyal/Desktop/BD_Proj/json/inp_predict.json /"user/arshgoyal/json"
# hdfs dfs -mkdir /"csv"

import json
import sys

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import array_contains, col, lit, struct, when
from pyspark.sql.types import *
from pyspark.sql.types import (ArrayType, IntegerType, StringType, StructField,
                               StructType)
from pyspark.streaming import StreamingContext

####################################################################################################################
########################################### Spark Initialisation ###################################################
####################################################################################################################
conf = SparkConf()
conf.setAppName("FPL")
sc = SparkContext(conf=conf)
sqlContext = SparkSession.builder.getOrCreate()
ssc = StreamingContext(sc, 5)
ssc.checkpoint("checkpoint_FPL")

##########################################################################################################################################
# Reading csv files
players_df = sqlContext.read.load("players.csv", format="csv", header="true", inferSchema="true")
teams_df = sqlContext.read.load("teams.csv", format="csv", header="true", inferSchema="true")


##########################################################################################################################################
# Helper Function to check valid team combination
def check_valid(team_positions):
    if team_positions['GK'] != 1:
        return False
    if team_positions['DF'] < 3:
        return False
    if team_positions['MD'] < 2:
        return False
    if team_positions['FW'] < 1:
        return False
    return True


##########################################################################################################################################
# Function to predcit winner
def predict_winner():
    with open('request_response_data/request/inp_predict.json') as f:
        input_data = json.load(f)

    # with open('/home/arshgoyal/Desktop/BD_Proj/Evaluation_Requests/Pred/inp1.json') as f:
    #     input_data = json.load(f)

    team1 = input_data['team1']
    team2 = input_data['team2']
    team1_name = team1['name']
    team2_name = team2['name']

    team1_positions = {'GK': 0, 'DF': 0, 'MD': 0, 'FW': 0}
    team2_positions = {'GK': 0, 'DF': 0, 'MD': 0, 'FW': 0}

    for i in range(1, 12):
        name = team1['player' + str(i)]
        row = players_df.filter(players_df.name == name)
        # print(name)
        role = row.collect()[0].role
        if role not in team1_positions:
            team1_positions[role] = 1
        else:
            team1_positions[role] += 1

    for i in range(1, 12):
        name = team2['player' + str(i)]
        row = players_df.filter(players_df.name == name)
        role = row.collect()[0].role
        if role not in team2_positions:
            team2_positions[role] = 1
        else:
            team2_positions[role] += 1

    # if not check_valid(team1_positions) or not check_valid(team2_positions):
    #     print('Invalid')
    #     return

    else:
        # regression check for player rating.

        with open('chemistry.json') as json_file:
            players_combination = json.load(json_file)

        # player_stats = sqlContext.read.json('/user/arshgoyal/Player_Rating.json/part-00000-5ce2d264-8688-4a91-95bd-1ea5d54d3f0d-c000.json')
        player_stats = sqlContext.read.json('Player_Rating.json')

        team1_player_ids = []
        team2_player_ids = []

        for i in range(1, 12):
            name1 = team1['player' + str(i)]
            row = players_df.filter(players_df.name == name1)
            player_id = row.collect()[0].Id
            team1_player_ids.append(player_id)

            name2 = team2['player' + str(i)]
            row = players_df.filter(players_df.name == name2)
            player_id = row.collect()[0].Id
            team2_player_ids.append(player_id)

        # print(team1_player_ids)

        team1_player_chemistries = {}
        team2_player_chemistries = {}
        for i in range(1, 12):
            team1_player_chemistries[team1_player_ids[i - 1]] = []
            team2_player_chemistries[team2_player_ids[i - 1]] = []

        for i in range(11):
            for j in range(i + 1, 11):
                if str(team1_player_ids[i]) + ' ' + str(team1_player_ids[j]) in players_combination:
                    team1_player_chemistries[team1_player_ids[i]].append(
                        float(players_combination[str(team1_player_ids[i]) + ' ' + str(team1_player_ids[j])]))
                    team1_player_chemistries[team1_player_ids[j]].append(
                        float(players_combination[str(team1_player_ids[i]) + ' ' + str(team1_player_ids[j])]))

                else:
                    team1_player_chemistries[team1_player_ids[i]].append(
                        float(players_combination[str(team1_player_ids[j]) + ' ' + str(team1_player_ids[i])]))
                    team1_player_chemistries[team1_player_ids[j]].append(
                        float(players_combination[str(team1_player_ids[j]) + ' ' + str(team1_player_ids[i])]))

                if str(team2_player_ids[i]) + ' ' + str(team2_player_ids[j]) in players_combination:
                    team2_player_chemistries[team2_player_ids[i]].append(
                        float(players_combination[str(team2_player_ids[i]) + ' ' + str(team2_player_ids[j])]))
                    team2_player_chemistries[team2_player_ids[j]].append(
                        float(players_combination[str(team2_player_ids[i]) + ' ' + str(team2_player_ids[j])]))
                else:
                    team2_player_chemistries[team2_player_ids[i]].append(
                        float(players_combination[str(team2_player_ids[j]) + ' ' + str(team2_player_ids[i])]))
                    team2_player_chemistries[team2_player_ids[j]].append(
                        float(players_combination[str(team2_player_ids[j]) + ' ' + str(team2_player_ids[i])]))

        team1_consolidated_chemistries = {key: (sum(value) / 10.0) for key, value in team1_player_chemistries.items()}
        team2_consolidated_chemistries = {key: (sum(value) / 10.0) for key, value in team2_player_chemistries.items()}

        for i in player_stats.collect():
            if i.Id in team1_consolidated_chemistries:
                # print(team1_consolidated_chemistries[i.Id])
                # print(i.ratings._2)
                # print("multiple: "+str(team1_consolidated_chemistries[i.Id]*i.ratings._2))
                team1_consolidated_chemistries[i.Id] = team1_consolidated_chemistries[i.Id] * i.ratings._2
            elif i.Id in team2_consolidated_chemistries:
                # print(team2_consolidated_chemistries[i.Id])
                # print(i.ratings._2)
                # print("multiple: " + str(team2_consolidated_chemistries[i.Id]*i.ratings._2))
                team2_consolidated_chemistries[i.Id] = team2_consolidated_chemistries[i.Id] * i.ratings._2

        strength_team1 = sum(list(team1_consolidated_chemistries.values()))
        # print(strength_team1)
        strength_team2 = sum(list(team2_consolidated_chemistries.values()))
        # print(strength_team2)

        print("sum team1: " + str(strength_team1))
        print("sum team2: " + str(strength_team2))

        winning_chance_team1 = (0.5 + ((strength_team1 - strength_team2) / (strength_team1 + strength_team2))) * 100
        winning_chance_team2 = (0.5 + ((strength_team2 - strength_team1) / (strength_team1 + strength_team2))) * 100
        # print(winning_chance_team1)

        out_predict = {}
        out_predict['team1'] = {"name": team1_name, "winning chance": winning_chance_team1}
        out_predict['team2'] = {"name": team2_name, "winning chance": winning_chance_team2}
        print(out_predict)
        print('##############################################################################')
        with open("request_response_data/response/out_predict.json", "w") as outfile:
            json.dump(out_predict, outfile)

        ##########################################################################################################################################


# Function to calculate Player Profile
def player_profile():
    # query = sqlContext.read.json("/user/arshgoyal/json/inp_player.json", multiLine = True)
    query = sqlContext.read.json("request_response_data/request/inp_player.json", multiLine=True)
    name = query.collect()[0].name

    new_df = players_df.filter(players_df.name == name)
    new_df.show()

    player_Id = new_df.collect()[0].Id
    # print(player_Id)

    player_Profile = sqlContext.read.json("Player_Profile.json")
    # player_Profile = sqlContext.read.json("/user/arshgoyal/Player_Profile.json/part-00000-a2ee9c95-d1f1-47a4-936c-24bb716a51a6-c000.json")
    # player_Profile.show()

    player_Profile = player_Profile.filter(player_Profile.Id == player_Id)
    player_Profile.show()

    metrics = player_Profile.collect()[0].metrics
    fouls = metrics._2
    goals = metrics._3
    own_goals = metrics._4
    pass_accuracy = metrics._5
    shots_target = metrics._6
    # print(metrics)

    new_df = new_df.withColumn("fouls", lit(fouls))
    new_df = new_df.withColumn("goals", lit(goals))
    new_df = new_df.withColumn("own_goals", lit(own_goals))
    new_df = new_df.withColumn("percent_pass_accuracy", lit(pass_accuracy))
    new_df = new_df.withColumn("percent_shots_on_target", lit(shots_target))

    new_df.show()

    new_df.write.json("request_response_data/response/out_player.json", "overwrite")


##########################################################################################################################################
# Function to Output match details
def match_details():
    query = sqlContext.read.json("request_response_data/request/inp_match.json", multiLine=True)
    # query = sqlContext.read.json("file:///home/arshgoyal/Desktop/BD_Proj/json/inp_match_cust1.json", multiLine = True)
    query.show()

    date = query.collect()[0].date
    label = query.collect()[0].label

    # print(date)
    # print(label)
    # print("######################################")

    match_Details = sqlContext.read.json("Match_Details.json")
    # match_Details = sqlContext.read.json("/user/arshgoyal/Match_details.json/part-00000-0ef259c2-3866-41a4-a191-343dc0aa987a-c000.json")
    match_Details.show()

    details = ''
    for i in match_Details.collect():
        if (date == i.match_Id[0]):
            if (label == i.match_Id[1]):
                details = i.details
                break

    '''
    Row(date='2017-08-12', duration='Regular', gameweek='1', 
    goals='[[214654, 1673, 2]]', 
    label='Crystal Palace - Huddersfield Town, 0 - 3', 
    own_goals='[[8221, 1628, 1]]', 
    red_cards='[]', 
    venue='Selhurst Park', 
    winner='1673', 
    yellow_cards='[8142, 11078, 279709, 214654]')
    '''

    date = details[0].date
    schema = StructType([StructField('date', StringType(), False)])
    new_df = sqlContext.createDataFrame(sqlContext.sparkContext.emptyRDD(), schema)
    l = [[date]]

    temp = sqlContext.createDataFrame(l)
    new_df = new_df.union(temp)

    new_df = new_df.withColumn("duration", lit(details[0].duration))
    new_df = new_df.withColumn("duration", lit(details[0].gameweek))

    if (details[0].winner != '0'):
        temp = teams_df.filter(teams_df.Id == details[0].winner)
        new_df = new_df.withColumn("winner", lit(temp.collect()[0].name))
    else:
        l = "NULL"
        new_df = new_df.withColumn("winner", lit(l))

    new_df = new_df.withColumn("gameweek", lit(details[0].gameweek))

    goals = details[0].goals
    goals = goals.replace('[', '')
    goals = goals.replace(']', '')
    goals = [goals.split(',')]
    # print(goals)
    # print("#############################")

    if (len(goals[0]) == 1):
        l = "NONE"
    else:
        l = []
        for i in goals:
            temp = players_df.filter(players_df.Id == i[0])
            name = temp.collect()[0].name
            temp = teams_df.filter(teams_df.Id == i[1])
            team = temp.collect()[0].name
            goal = i[2]
            dic = {"name": name, "team": team, "number_of_goals": goal}
            l.append(dic)
        # print(l)
        l = str(l)

    new_df = new_df.withColumn("goals", lit(l))

    own_goals = details[0].own_goals
    own_goals = own_goals.replace('[', '')
    own_goals = own_goals.replace(']', '')
    own_goals = [own_goals.split(',')]
    # print(len(own_goals[0]))
    # print("#############################")

    if (len(own_goals[0]) == 1):
        l = "NONE"
    else:
        l = []
        for i in own_goals:
            temp = players_df.filter(players_df.Id == i[0])
            name = temp.collect()[0].name
            temp = teams_df.filter(teams_df.Id == i[1])
            team = temp.collect()[0].name
            goal = i[2]
            dic = {"name": name, "team": team, "number_of_goals": goal}
            l.append(dic)
        l = str(l)
    new_df = new_df.withColumn("own_goals", lit(l))

    yellow_cards = details[0].yellow_cards
    yellow_cards = yellow_cards.replace('[', '')
    yellow_cards = yellow_cards.replace(']', '')
    yellow_cards = yellow_cards.split(',')

    if (len(yellow_cards) == 1):
        l = "NONE"
    else:
        l = []
        for i in yellow_cards:
            temp = players_df.filter(players_df.Id == i)
            name = temp.collect()[0].name
            l.append(name)
        print(l)
        l = str(l)

    new_df = new_df.withColumn("yellow_cards", lit(l))

    red_cards = details[0].red_cards
    red_cards = red_cards.replace('[', '')
    red_cards = red_cards.replace(']', '')
    red_cards = red_cards.split(',')
    # print(red_cards)
    # print(len(red_cards))

    if (len(red_cards) == 1):
        l = "NONE"
    else:
        l = []
        for i in red_cards:
            temp = players_df.filter(players_df.Id == i)
            name = temp.collect()[0].name
            l.append(name)
        # print(l)
        l = str(l)
    new_df = new_df.withColumn("red_cards", lit(l))
    new_df.show()

    new_df.write.json("request_response_data/response/out_match.json", "overwrite")


def get_match_rate_detail():
    query = sqlContext.read.json("request_response_data/request/inp_player_match_rate.json", multiLine=True)
    name = query.collect()[0].name

    new_df = players_df.filter(players_df.name == name)
    new_df.show()

    player_Id = new_df.collect()[0].Id
    # print(player_Id)

    player_Rate = sqlContext.read.json("Player_Match_Rating.json")
    # player_Profile = sqlContext.read.json("/user/arshgoyal/Player_Profile.json/part-00000-a2ee9c95-d1f1-47a4-936c-24bb716a51a6-c000.json")
    # player_Profile.show()

    player_Rate = player_Rate.filter(player_Rate.Id == player_Id)
    player_Rate.show()

    rating = player_Rate.collect()[0].ratings
    rate = rating._2

    rate_df = sqlContext.createDataFrame([(str(name), float(10 * rate))], ["name", "performance"])
    rate_df.show()

    rate_df.write.json("request_response_data/response/out_player_match_rate.json", "overwrite")


##########################################################################################################################################
query = sqlContext.read.load("request_response_data/request/inp_predict.json", format="json", multiLine=True)
# query = sqlContext.read.json("inp1.json", multiLine = True)
query.show()
try:
    flag = query.collect()[0].req_type
except:
    flag = 3
if (flag != 3 and query.collect()[0].req_type):
    req_type = query.collect()[0].req_type
    if req_type == 1:
        # print("************************************************")
        predict_winner()
    elif req_type == 4:
        get_match_rate_detail()
    elif req_type == 2:
        player_profile()
else:
    match_details()
    print("#######################################################")
##########################################################################################################################################
