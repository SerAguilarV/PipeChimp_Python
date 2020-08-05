from http.server import HTTPServer, BaseHTTPRequestHandler
import threading
import time
from urllib import parse
import json
import requests

estado = "false"
sectors = []
timeExp = 10
URL_PIPEDRIVE = "" 
APIKEY_PIPEDRIVE = "" 

URL_MAILCHIMP  = ""
APIKEY_MAILCHIMP = ""

def f():
    global estado, sectors, timeExp
    while True:
        if estado == "true":
            print("-- Sectors export starting: {}".format(sectors))
            # time.sleep(timeExp)
            exportAudience()
            print("-- Export finished ... waiting {} seconds".format(timeExp))
            time.sleep(timeExp)
        # if estado != "false"
            # print("Hello")

class GetHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        global estado, sectors
        cl = int(self.headers.get('Content-Length'))
        body = json.loads(self.rfile.read(cl).decode('utf8').replace("'",'"'))
        print("-- Body {}".format(body))
        estado = body['state']
        sectors = body['sectors']
        self.send_response(200)
        self.send_header('Content-Type',
                         'text/plain; charset=utf-8')
        self.end_headers()
        message = "Received"
        self.wfile.write(message.encode('utf-8'))

def server():
    PORT = 8085
    with HTTPServer(("", PORT), GetHandler) as httpd:
        print("- Serving at port", PORT)
        httpd.serve_forever()

def getSectors(SectorsOrgs):
    sectors = {
        "28":	"Operador",
        "15":	"Park / Other",
        "16":	"Parking Company",
        "78":	"Hotel",
        "13":	"Real Estate / Constructora",
        "72":	"Retailer",
        "65":	"Shopping Malls",
        "64":	"Supermercados",
        "29":	"Supplier",
        "69":	"Universidades",
        "11":	"Promotor/Desarrollador",
        "12":	"Firma Architectura",
        "17":	"Office Tower",
        "66":	"Mix Use",
        "26":	"Integración",
        "70":	"Hospitales",
        "14":	"Association",
        "71":	"Aeropuertos"
    }
    sectorFinal = ""
    for sec in SectorsOrgs.split(","):
        sectorFinal += sectors[sec] + ", "
    return sectorFinal[:-2]

def getOrganizations():
    global APIKEY_PIPEDRIVE, URL_PIPEDRIVE
    organizations = {}
    flag = 500
    cont = 0
    while flag == 500:
        params_PIPE = {
            "api_token": APIKEY_PIPEDRIVE,
            "limit": "500",
            "start" : str((cont * 500))
            }
        res = requests.get(
            url = URL_PIPEDRIVE + "organizations/",
            params= params_PIPE,
        )
        orgs = res.json()["data"]
        for org in orgs:
            if org["db1de7f663b5ba7cedfb927dd811b0c7693c33b6"]:
                organizations[org["id"]] = {
                            "name" : org["name"],
                            "sector" : getSectors(org["db1de7f663b5ba7cedfb927dd811b0c7693c33b6"]),
                            "web" : org["8c2356d53aa262f0eabc4b5c7b6d957fd7714861"]
                        }
        flag = len(orgs)
        cont +=1
    print("** Numero de Organizaciones: {}".format(len(organizations)))
    return organizations

def getPeople(Organizations):
    global APIKEY_PIPEDRIVE, URL_PIPEDRIVE
    People = []
    flag = 500
    cont = 0
    while flag == 500:
        params_PIPE = {
            "api_token": APIKEY_PIPEDRIVE,
            "limit": "500",
            "start" : str((cont * 500))
            }
        res = requests.get(
            url = URL_PIPEDRIVE "persons/",
            params= params_PIPE,
        )
        PeoplePD = res.json()["data"]
        for person in PeoplePD:
            if person["org_id"]:
                if person["org_id"]["value"] in Organizations:
                    People.append(
                        {
                            "id": person["id"],
                            "first_name" : person["first_name"],
                            "last_name" : person["last_name"],
                            "email" : person["email"][0]["value"],
                            "organization" : Organizations[person["org_id"]["value"]]
                        }
                    )
        flag = len(PeoplePD)
        cont +=1
    print("** Numero de personas: {}".format(len(People)))
    return People

def importarAudiencia(Audiencia):
    global sectors, URL_MAILCHIMP, APIKEY_MAILCHIMP
    headers = {"Authorization": APIKEY_MAILCHIMP }
    added = []
    for person in Audiencia:
        for sector in sectors:
            if sector in person["organization"]["sector"]:
                jsonPD = {
                    "email_address": person["email"],
                    "status" : "subscribed",
                    "merge_fields" : {
                    "FNAME": person["first_name"],
                    "LNAME": person["last_name"]}
                }
                res = requests.post(URL_MAILCHIMP + "lists/89c8eb6e41/members/", headers = headers, json = jsonPD)
                if res.status_code == 200:
                    print("** Persona Agregada:   Nombre: {}   Correo: {}".format(
                        person["first_name"] + " " +person["last_name"], person["email"]))
                    added.append(person)
                else:
                    print("** Persona no Agregada:   Nombre: {}   Correo: {}".format(
                        person["first_name"] + " " +person["last_name"], person["email"]))
                    added.append(person)
                # time.sleep(1)
    tags = getSectorsTags()
    for tag in tags:
        id = tag["id"]
        name = tag["name"]
        for person in added:
            if name in person["organization"]["sector"]:
                res = requests.post(URL_MAILCHIMP + "lists/89c8eb6e41/segments/{}/members/".format(
                    id
                ), headers = headers, json={"email_address" : person["email"]})
                if res.status_code == 200:
                    print("** Tag added {} to {}".format(name, person["email"]))
                else:
                    print("** Tag not added {} to {}".format(name, person["email"]))
    return added

def getSectorsTags():
    global sectors, APIKEY_MAILCHIMP, URL_MAILCHIMP
    headers = {"Authorization": APIKEY_MAILCHIMP}
    res = requests.get(URL_MAILCHIMP + "lists/89c8eb6e41/segments", headers = headers)
    if res.status_code == 200:
        segmentosJSON = res.json()["segments"]
        segmentos = []
        for seg in range(len(segmentosJSON)):
            segmentos.append(
                {"id" : segmentosJSON[seg]["id"], "name" : segmentosJSON[seg]["name"]}
            )
    tagsReturn = []
    for sec in sectors:
        for seg in segmentos:
            if sec == seg["name"]:
                tagsReturn.append(
                    seg
                )
    return tagsReturn


def exportAudience():
    Organizations = getOrganizations()
    # print(Organizations)
    People = getPeople(Organizations)
    # print(People)
    Peopleadded = importarAudiencia(People)
    # print(Peopleadded)

if __name__ == '__main__':
    p = threading.Thread(target=f,)
    p.start()
    p2 = threading.Thread(target=server,)
    p2.start()
    p.join()
    p2.join()
