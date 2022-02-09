from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from psycopg2.extensions import AsIs
import pandas.io.sql as sqlio
import numpy as np
import pandas as pd
import psycopg2

from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import normalize
from sklearn.decomposition import PCA
from fastapi.responses import JSONResponse

app = FastAPI()

conn = psycopg2.connect(
    host="cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com",
    database="cmdb",
    user="cm",
    password="cm")
cursor=conn.cursor()

# print(conn)


@app.get('/')
def get_hello():
    return {'message': 'return hello'}


origins = [
    'http://localhost:3000'
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*']
)

query="select tp_id from db_actions limit 20"
cursor.execute(query)
print(cursor.fetchall())

def get_CL_LatLng(user_tp_id):
    query="select lat,lng from db_actions where tp_id=%s and lat is not null and lng is not null and action=%s"
    cursor.execute(query,(user_tp_id,"DB_MARKED_DELIVERY",))
    latlng=pd.DataFrame(cursor.fetchall(),columns=['lat','lng'])
    X=latlng

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    X_normalized = normalize(X_scaled)
    X_normalized = pd.DataFrame(X_normalized)

    db_default = DBSCAN(eps=0.0025, min_samples=5).fit(X)
    labels = db_default.labels_

    set1 = set()

    lables_list = []
    for i in range(len(labels)):
        lables_list.append(labels[i].item())
        set1.add(lables_list[i])

    return {
        'lat': latlng['lat'],
        'lng': latlng['lng'],
        'labels': lables_list,
        'noofclusters': len(set1)
    }

def get_address_lat_lng(id: str):
    query="select address from team_leaders where id=%s"
    cursor.execute(query,(id,))
    address_id=cursor.fetchall()[0][0]
    query="select lat,lng from user_addresses where id=%s"
    cursor.execute(query,(address_id,))
    tmp=cursor.fetchall()[0]
    # print(tmp[0])
    # print(type(tmp[0]))
    return {
        'lat':tmp[0],
        'lng':tmp[1]
    }

def get_spoke_locations(tp_id:str):
    query="select metadata from db_actions where action=%s and tp_id=%s"
    cursor.execute(query,("DB_MARKED_DELIVERY",tp_id,))
    tmp=cursor.fetchall()
    route_ids=[]
    print(len(tmp))
    for i in range(len(tmp)):
        route_ids.append(tmp[i][0]['route_id'])

    set_spokeand_ware=set()

    for i in range(len(route_ids)):
        query="select spoke_name from routes where id=%s"
        cursor.execute(query,(route_ids[i],))
        tmp=cursor.fetchall()[0][0]
        set_spokeand_ware.add(tmp)

    list_spokeand_ware=list(set_spokeand_ware)
    df=pd.DataFrame(columns=['lat','lng'])

    for i in range(len(list_spokeand_ware)):
        query="select lat,lng from warehouses where warehouse_name=%s"
        cursor.execute(query,(list_spokeand_ware[i],))
        tmp=cursor.fetchall()[0]
        df1=pd.DataFrame({'lat':[tmp[0]],
                          'lng':tmp[1]})
        df=pd.concat([df,df1],ignore_index=True)
    df.reset_index(drop=True)

    return df


@app.get('/{id}')
def get_data_by_tp_id(id: str):
    return {
        'tp_id': id,
        'data': get_CL_LatLng(id),
        'add_latlng': get_address_lat_lng(id),
        'spoke':get_spoke_locations(id)
    }



# get_CL_LatLng("poonambhatia9319317323")

