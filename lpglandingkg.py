#!/usr/bin/env python
from json import dumps
import logging
import os

from flask import (
    Flask,
    g,
    request,
    Response,
)
from neo4j import (
    GraphDatabase,
    basic_auth,
)


app = Flask(__name__, static_url_path="/static/")

url = 'bolt://172.25.68.162:7687'
username = 'neo4j'
password = 'Petronas@123456'
neo4j_version = os.getenv("NEO4J_VERSION", "4")
database = 'lpglandingkg'

port = 1433

driver = GraphDatabase.driver(url, auth=basic_auth(username, password))


def get_db():
    if not hasattr(g, "neo4j_db"):
        if neo4j_version >= "4":
            g.neo4j_db = driver.session(database=database)
        else:
            g.neo4j_db = driver.session()
    return g.neo4j_db


@app.teardown_appcontext
def close_db(error):
    if hasattr(g, "neo4j_db"):
        g.neo4j_db.close()


@app.route("/")
def get_index():
    return app.send_static_file("index.html")


def serialize_equipmain(EquipmentMaintenance):
    return {
        "id": EquipmentMaintenance["id"],
        "ABCindicatorfortechnicalobject": EquipmentMaintenance["ABCindicatorfortechnicalobject"],
        "CatalogProfile": EquipmentMaintenance["CatalogProfile"],
        "CostCenter": EquipmentMaintenance["CostCenter"],
        "CurrencyKey": EquipmentMaintenance["CurrencyKey"],
        "EquipmentCategory": EquipmentMaintenance["EquipmentCategory"],
        "EquipmentNumber": EquipmentMaintenance["EquipmentNumber"],
        "Equipmentacquisitionvalue": EquipmentMaintenance["Equipmentacquisitionvalue"],
        "FunctionalLocation": EquipmentMaintenance["FunctionalLocation"],
        "Inventorynumber": EquipmentMaintenance["Inventorynumber"],
        "Locationofmaintenanceobject": EquipmentMaintenance["Locationofmaintenanceobject"],
        "Maintenanceplannergroup": EquipmentMaintenance["Maintenanceplannergroup"],
        "Maintenanceplant": EquipmentMaintenance["Maintenanceplant"],
        "Material": EquipmentMaintenance["Material"],
        "Plantsection": EquipmentMaintenance["Plantsection"],
        "Room": EquipmentMaintenance["Room"],
        "StartUpDate": EquipmentMaintenance["StartUpDate"],
        "SystemStatus": EquipmentMaintenance["SystemStatus"],
        "UserStatus": EquipmentMaintenance["UserStatus"]
    }
    

def serialize_abom(abom):
    return {
        "AssetBillOfMaterialId": abom[0],
        "BOMComponent": abom[1],
        "BOMItemNumber": abom[2],
        "EquipmentNumber": abom[3],
        "ItemCategoryBillOfMaterial": abom[4],
        "ValidFromDate": abom[5]
    }


def serialize_assmain(AssetMaintenance):
    return {
        "AssetMaintenanceId": AssetMaintenance["AssetMaintenanceId"],
        "ControllingArea": AssetMaintenance["ControllingArea"],
        "CostCenter": AssetMaintenance["CostCenter"],
        "EquipmentNumber": AssetMaintenance["EquipmentNumber"],
        "FunctionalLocation": AssetMaintenance["FunctionalLocation"],
        "MaintenancePlanningPlant": AssetMaintenance["MaintenancePlanningPlant"],
        "MaintenancePlantRoom": AssetMaintenance["MaintenancePlantRoom"]
    }

@app.route("/graph")
def get_graph():
    def work(tx, limit):
        return list(tx.run(
            "MATCH (e:EquipmentMaintenance)<-[:EquipmentNumber]-(b:AssetBillOfMaterial) "
            "RETURN e.Maintenanceplant AS Plant, collect(b.AssetBillOfMaterialId) AS ABOMId "
            "LIMIT $limit",
            {"limit": limit}
        ))

    db = get_db()
    results = db.read_transaction(work, request.args.get("limit", 100))
    nodes = []
    rels = []
    i = 0
    for record in results:
        nodes.append({"title": record["Plant"], "label": "EquipmentMaintenance"})
        target = i
        i += 1
        for AssetBillOfMaterialId in record["ABOMId"]:
            AssetBillOfMaterial = {"title": AssetBillOfMaterialId, "label": "AssetBillOfMaterial"}
            try:
                source = nodes.index(AssetBillOfMaterial)
            except ValueError:
                nodes.append(AssetBillOfMaterial)
                source = i
                i += 1
            rels.append({"source": source, "target": target})
    return Response(dumps({"nodes": nodes, "links": rels}),
                    mimetype="application/json")


@app.route("/search")
def get_search():
    def work(tx, q_):
        return list(tx.run(
            "MATCH (em:EquipmentMaintenance) "
            "WHERE toLower(em.Maintenanceplant) CONTAINS toLower($Maintenanceplant) "
            "RETURN em",
            {"Maintenanceplant": q_}
        ))

    try:
        q = request.args["q"]
    except KeyError:
        return []
    else:
        db = get_db()
        results = db.read_transaction(work, q)
        return Response(
            dumps([serialize_movie(record["em"]) for record in results]),
            mimetype="application/json"
        )


@app.route("/movie/<title>")
def get_movie(title):
    def work(tx, title_):
        return tx.run(
            "MATCH (movie:Movie {title:$title}) "
            "OPTIONAL MATCH (movie)<-[r]-(person:Person) "
            "RETURN movie.title as title,"
            "COLLECT([person.name, "
            "HEAD(SPLIT(TOLOWER(TYPE(r)), '_')), r.roles]) AS cast "
            "LIMIT 1",
            {"title": title_}
        ).single()

    db = get_db()
    result = db.read_transaction(work, title)

    return Response(dumps({"title": result["title"],
                           "cast": [serialize_cast(member)
                                    for member in result["cast"]]}),
                    mimetype="application/json")


# @app.route("/movie/<title>/vote", methods=["POST"])
# def vote_in_movie(title):
#     def work(tx, title_):
#         return tx.run(
#             "MATCH (m:Movie {title: $title}) "
#             "SET m.votes = coalesce(m.votes, 0) + 1;",
#             {"title": title_}
#         ).consume()

#     db = get_db()
#     summary = db.write_transaction(work, title)
#     updates = summary.counters.properties_set

#     db.close()

#     return Response(dumps({"updates": updates}), mimetype="application/json")


if __name__ == "__main__":
    logging.root.setLevel(logging.INFO)
    logging.info("Starting on port %d, database is at %s", port, url)
    app.run(port=port)
