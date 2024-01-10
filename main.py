# -*- coding: utf-8 -*-
"""
Created on Mon Oct 30 18:07:57 2023

@author: DAVI
"""
from flask import Flask, jsonify, request, make_response, render_template, redirect, session
#from Banco import Dados
import pyodbc
import pandas as pd 
import requests 
from datetime import datetime
from google.oauth2 import service_account
from google.cloud import storage
import os


app = Flask(__name__)
app.config['SECRET_KEY'] ='thisisthesecretkey'
app.config['JSON_AS_ASCII'] = False
BUCKET_NAME = 'dev-lane-403118.appspot.com'
CREDENCIALS = 'key.json'


def Insert_SQL(df): 
        driver = '/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.4.so.1.1'
        client = storage.Client.from_service_account_json(CREDENCIALS)
        current_path_directory = os.getcwd()
        bucket = client.get_bucket(BUCKET_NAME)
        blob = bucket.blob('/msodbcsql17/lib64/libmsodbcsql-17.10.so.5.1')
        #print(f'{blob}')
        
        conn = pyodbc.connect(f'Driver={driver};'
                              'Server=bibd.fleury.com.br;'
                              'Database=BDSTAGE;'
                              'UID=usr_python;'
                              'PWD=BICorporativo#10;'
                              'Trusted_Connection=yes;')

        banco = conn.cursor()
        banco.execute(""" 
                          INSERT 
                          INTO      BDSTAGE.dbo.ST_TB_CORRIDAS_UBER_COUB_teste      
                          VALUES    (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
                                          ?,?,?,?,?,?,?,?,?,?,?,?,?,?,
                                          ?,?,?,?,?,?,?,?,?,?,?,?,?) 
                          
                                  """  
                                       ,str(df['trip_uuid']),
                                        str(datetime.strptime(df['transaction_history'][0]['utc_timestamp'],"%Y-%m-%dT%H:%M:%S.%fZ")),
                                        str(datetime.strptime(df['transaction_history'][0]['utc_timestamp'],"%Y-%m-%dT%H:%M:%S.%fZ").strftime("%d/%m/%Y")),  
                                        str(datetime.strptime(df['transaction_history'][0]['utc_timestamp'],"%Y-%m-%dT%H:%M:%S.%fZ").strftime("%H:%M:%S")),
                                        str(datetime.utcfromtimestamp(df['pickup']['time']['unix_timestamp']).strftime("%d/%m/%Y")),
                                        str(datetime.utcfromtimestamp(df['pickup']['time']['unix_timestamp']).strftime("%H:%M:%S")),
                                        str(datetime.strptime(df['dropoff']['time']['utc_timestamp'],"%Y-%m-%dT%H:%M:%S.%fZ").strftime("%d/%m/%Y")),
                                        str(datetime.strptime(df['dropoff']['time']['utc_timestamp'],"%Y-%m-%dT%H:%M:%S.%fZ").strftime("%H:%M:%S")),
                                        str(datetime.utcfromtimestamp(df['dropoff']['time']['unix_timestamp']).strftime("%d/%m/%Y")),
                                        str(datetime.utcfromtimestamp(df['dropoff']['time']['unix_timestamp']).strftime("%H:%M:%S")),
                                        str(df['pickup']['time']['utc_offset']),
                                        str(df['given_name']),
                                        str(df['family_name']),
                                        str(df['email']),
                                        str(df['employee_id']),
                                        str(df['expense_code']),
                                        str(df['pickup']['location']['city']),
                                        str(df['distance']),
                                        str(df['duration']),
                                        str(df['pickup']['location']['address']),
                                        str(df['dropoff']['location']['address']),
                                        str(df['expense_code']),
                                        '',
                                        str(df['invoices']),
                                        str(df['program_name']),
                                        str(df['organization_name']),
                                        '',
                                        str(df['transaction_history'][0]['transaction_type_detail']),
                                        str(df['total_charged']),
                                        str(df['transaction_history'][0]['taxes']),
                                        str(df['total_owed']),
                                        str(df['transaction_history'][0]['amount']),
                                        str(df['transaction_history'][0]['currency_code']),
                                        str(df['transaction_history'][0]['net_fare']),
                                        str(df['taxes']),
                                        str(df['total_owed']),
                                        str(df['total_charged']),
                                        str(df['taxes']),
                                        str(df['transaction_history'][0]['transaction_uuid']),
                                        str(df['vat_amount']),
                                        str(df['organization_name']),
                                        str(df['product_name'])
                                           
                                        )
        
        banco.commit()
        banco.close()
                                       
        return 'Dados Inseridos com sucesso!'

def get_Receive(trip_uid): 
        def get_Token(): 
                client_ID='xu9a9wFtl3bI76HW18HOnnCRHaraSJEC'
                client_secret='4XK2UO9Gqupn1dDCVfjRoZTTp8-XVSqoQ6vOwCzq'
                gt='client_credentials'
                scp = 'business.receipts'
                url = f'https://login.uber.com/oauth/v2/token?client_id={client_ID}&client_secret={client_secret}&grant_type={gt}&scope={scp}'
                
                access_token = requests.post(url=url,verify=False).json()['access_token']
                
                return access_token
                
        headers = {'Authorization':f'Bearer {get_Token()}',
                   'scope':'business.receipts'
            
            }
        url= f'https://api.uber.com/v1/business/trips/{trip_uid}/receipt'    
        df = requests.get(url=url, headers=headers,verify=False).json()
        Insert_SQL(df)
        

@app.route('/')
def index(): 
    return jsonify({'Seja bem vindo a escuta/webhook UberPay':100000})


#@app.route('/banco')
#def banco():
    #return make_response(jsonify(Dados))

@app.route('/webhook', methods=['POST'])
def webhook():
    dados = request.get_json(force=True) 
    #Dados.append(dados)
    trip_uid = dados['meta']['resource_id']
    get_Receive(trip_uid)
    
    return make_response(jsonify(dados))




if __name__ == '__main__':
    app.run(use_reloader=False,debug = False)
