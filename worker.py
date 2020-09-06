import requests
from bs4 import BeautifulSoup
import csv
import pandas as pd
import re
import numpy as np
import string
from flask import *
import sys

def preprocess(x):
    x=x.upper()
    x=x.replace(" ","")
    x='/'+x
    return x

def do_input(data):
    data=data.filter(['FPO Name','Registration No.'])
    data['FPO Name']=data['FPO Name'].apply(lambda x: preprocess(x) )
    data['Registration No.']=data['Registration No.'].apply(lambda x: preprocess(x) )
    data['url']=data['FPO Name']+data['Registration No.']
    final_list=list(data['url'])
    company_url= final_list
    return  company_url
#print(company_url)


def get_tables(htmldoc):
    soup = BeautifulSoup(htmldoc)
    all_tables = soup.findAll('table',{"class":"table table-striped"})
    charges = soup.findAll('table',{'id': "charges"})
    return all_tables,charges

def map_loan_details(rows):
    price_re = r'[0-9]+[,][0-9]+'
    price_ex = ''
    charge_id = r'[0-9]+'
    date_re = r'[0-9]{4}[-][0-9]{2}[-]{2}'
    charge_ex = ''
    bank = ''
    if (not re.findall(charge_id,rows['Charge ID'])) and not rows['Charge ID']!='NIL':
        rows['Charge ID'] = list(filter(charge_id.match, rows.values()))
    if (not re.findall(price_re,rows['Amount'])) and not rows['Amount']!='NIL':
        rows['Amount'] = list(filter(price_re.match,rows['Amount']))
    if (not re.findall(date_re,rows['Creation Date'])) and not rows['Creation Date']!='NIL':
        rows['Creation Date'] = list(filter(date_re.match,rows['Creation Date']))[0]
    if (not re.findall(rows['Modification Date'])) and not rows['Modification Date']!='NIL':
        rows['Modification Date'] = list(filter(date_re.match, rows['Modification Date']))[0]
    if (not re.findall(rows['Closure Date'])) and not rows['Closure Date']!='NIL':
        rows['Closure Date'] = list(filter(date_re.match, rows['Closure Date']))[0]

    return rows


def get_result(URL):
    # URL = 'https://www.zaubacorp.com/company/DHOLI-SAKRA-FARMERS-PRODUCER-COMPANY-LIMITED/U01404BR2017PTC034160'
    page = requests.get(URL)
    df = pd.DataFrame()
    tables,charges = get_tables(page.content)
    d = {}
    charges_dict = {}
    charges_cols = {}
    for table in charges:
        tr = table.findAll(['tr'])
        for t in tr:
            td = t.find_all('td')
            row = [i.text for i in td]
            if 'Charge ID' in row:
                charges_cols = dict.fromkeys(row,'NIL')
            elif len(charges_cols)>0:
                if len(charges_dict)>0 and row:
                    print('yessss')
                    for idx,(key,values) in enumerate(charges_dict.items()):
                        charges_dict[key]= values + ',' + row[idx]
                elif not 'No charges found' in row:
                    charges_dict = dict(zip(charges_cols.keys(),row))
                if len(row)>7:
                    print('lolll')
                    pass
    if len(charges_dict) > 0:
        pass
    # charges =
    for table in tables:
        tr = table.findAll(['tr'])
        for t in tr:
            td = t.find_all('td')
            row = [i.text.replace('\n', '') for i in td]
            if len(row) == 2:
                desc = row[1].replace(' ', '')
                if desc and row[0] != ' ':
                    d[row[0]] = [row[1]]
    # df.from_dict(d)
    if len(charges_dict)>0:
        d.update(charges_dict)
    else:
        d.update(charges_cols)
    columns = list(d.keys())
    df = pd.DataFrame(columns=columns)

    for cols in df.columns:
        val = d[cols]
        df[cols] = val
    #     print(df[cols].iloc[0])
    return df



if getattr(sys, 'frozen', False):
    template_folder = os.path.join(sys._MEIPASS, 'templates')
    static_folder = os.path.join(sys._MEIPASS, 'static')
    app = Flask(__name__, template_folder=template_folder, static_folder=static_folder)

else:
    app = Flask(__name__)

@app.route('/run',methods=["GET","POST"])
def run_worker():
    if request.method == "POST":
        f = request.form['upload-file']
        Base_URL = "https://www.zaubacorp.com/company"
        final_df = pd.DataFrame()
        input_file = pd.read_csv(f)
        company_url = do_input(input_file)
        for company in company_url:
            url = Base_URL + company
            data = get_result(url)
            c = company.split('/')[0]
            final_df = pd.concat([final_df, data], axis=0, ignore_index=True)
        final_df.to_csv('static/scrapped_'+f, index=False)
        return redirect('static/scrapped_'+f)


@app.route('/')
def start():
    return render_template('index.html')

def run_flask():
    app.run(host='127.0.0.1', port=5000)

if __name__ == '__main__':
    run_flask()
