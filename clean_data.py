import pandas as pd
from pymongo import MongoClient


# 显示所有列
pd.set_option('display.max_columns', None)
#显示所有行
pd.set_option('display.max_rows', None)
#设置value的显示长度为100，默认为50
pd.set_option('max_colwidth',1000)

def _connect_mongo(host, port, db):
    """ A util for making a connection to mongo """
    conn = MongoClient(host, port)
    return conn[db]


def read_mongo(db, collection, query={}, host='localhost', port=27017, no_id=True):
    """ Read from Mongo and Store into DataFrame """

    # Connect to MongoDB
    db = _connect_mongo(host=host, port=port, db=db)

    # Make a query to the specific DB and Collection
    cursor = db[collection].find(query)

    # Expand the cursor and construct the DataFrame
    df = pd.DataFrame(list(cursor),columns=["_id","id","name","directors","writers","actors","genres","countries","languages"
                                            ,"pubdates","durations","rating"])

    # Delete the _id
    if no_id:
        del df['_id']

    return df

def getPeople(actorlist):
    if len(actorlist):
        actor_list = []
        for i in actorlist[:5]:
            try:
                oneactor = i.get("name") + i.get("href").split("/")[-2]
            except:
                oneactor = i.get("name")
            actor_list.append(oneactor)
        return "|".join(actor_list)
    else:
        return ""

data = read_mongo("db","Douban")

# 数据预处理
# 去除空值
data = data.dropna()
data = data[data['actors'].map(lambda d: len(d)) > 0]
data = data[data['directors'].map(lambda d: len(d)) > 0]
data = data[data['writers'].map(lambda d: len(d)) > 0]
data = data[data['genres'].map(lambda d: len(d)) > 0]
data = data[data['countries'].map(lambda d: len(d)) > 0]
data = data[data['languages'].map(lambda d: len(d)) > 0]
data = data[data['pubdates'].map(lambda d: len(d)) > 0]
data = data[data['durations'].map(lambda d: len(d)) > 0]
data = data[data["name"].map(lambda d:len(d) > 0)]
data = data[data["pubdates"].map(lambda d:len(d) > 0)]

#将字典转成单列
data["Rate"] = data["rating"].apply(lambda x:x.get('average'))
data['Reviews_count'] = data["rating"].apply(lambda x:x.get('reviews_count'))
data["Actors"] = data["actors"].apply(getPeople)
data["Directors"] = data["directors"].apply(getPeople)
data["Writers"] = data["writers"].apply(getPeople)
data["Genres"] = data["genres"].apply(lambda x:"|".join(x[:5]))
data["Country"] = data["countries"].apply(lambda x:x[0])
data["date"] = data["pubdates"].apply(lambda x:x[0].split("(")[0])
data["date"] = pd.to_datetime(data['date'])
data["Year"] = data["date"].dt.year
data["DayOfYear"] = data["date"].dt.dayofyear
data["Languages"] = data["languages"].apply(lambda x:x[0])
data["Durations"] = data["durations"].apply(lambda x:x[0].replace("分钟",""))
data["Name"] = data["name"].apply(lambda x:x.split(" ")[0])
# del data[""]
data = data.drop(["pubdates","date","name","rating","actors","directors","writers","genres","countries","languages","durations"], axis=1)
data.to_csv("clean_data.csv",index=False)