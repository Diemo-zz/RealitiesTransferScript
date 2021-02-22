import os

import neo4j
import pandas

ORG_ID = "60313c03b732dd00483dab8b"


def add_on_create(field, value):
    if isinstance(value, str) and "'" in value:
      value = value.replace("'", "")
    new_command = f"ON CREATE SET p.{field}='{value}'\n"
    return new_command


def add_on_match(field, value):
    if isinstance(value, str) and "'" in value:
      value = value.replace("'", "")
    new_command = f"ON MATCH SET p.{field}='{value}'\n"
    return new_command


def run_command(tx, command):
    tx.run(command)


os.chdir("C://Users/diarm/Downloads")

driver = neo4j.GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "testpassword"))

df = pandas.ExcelFile("Kiezburn Realities.xlsx")


def format_users(df):
    users = df.parse("users", header=0)

    cols_to_drop = []
    for column in users.columns:
      if column.startswith("Unnamed"):
        cols_to_drop.append(column)

    users.drop(cols_to_drop, axis=1, inplace=True)

    rename = {"Index": "Index", "person id": "id", "person name": "new_name", "person email": "email"}

    new_names = [rename.get(n) for n in users.columns]
    users.columns = new_names
    return users


def update_user(user):
    command = f"""
    MERGE (p:Person {{email: '{user.email}' }})
      ON CREATE SET p.created = timestamp()"""
    if user.new_name:
      command += add_on_create("name", user.new_name)
    command += add_on_create("nodeId", user.id)
    if user.new_name:
      command += add_on_match("name", user.new_name)
    command += add_on_match("nodeId", user.id)
    command += """
      RETURN p.name, p.email, p.nodeId
                 """
    return command


def import_users(df):
    users = format_users(df)
    with driver.session() as session:
        for i, user in users.iterrows():
            command = update_user(user)
            session.write_transaction(run_command, command)


def format_needs(df):
    needs = df.parse("needs")
    cols_to_drop = []
    for column in needs.columns:
        if column.startswith("Unnamed"):
            cols_to_drop.append(column)
    needs.drop(cols_to_drop, inplace=True, axis=1)
    renamed_columns = ["id", "title", "description"]
    needs.columns = renamed_columns
    return needs


def update_need(need):
    command = f"""
    MERGE (p:Need {{nodeId: '{need.id}' }})
      ON CREATE SET p.created = timestamp()\n"""
    if need.description:
        command += add_on_create("description", need.description)
    if need.title:
        command += add_on_create("title", need.title)
    if need.description:
        command += add_on_match("description", need.description)
    if need.title:
        command += add_on_match("title", need.title)
    command += """
        RETURN p.title, p.nodeId
                 """
    return command


def connect_need_to_ord(need, orgId=ORG_ID):
    command = f"""MATCH (a:Org), (b:Need)
WHERE a.orgId = '{orgId}' and b.nodeId = '{need.id}'
CREATE (a) - [r:HAS] -> (b)
  """
    return command


def connect_guide_to_need(need_guide):
    command = f"""MATCH (a:Need), (b:Person) 
    WHERE a.nodeId = '{need_guide.NID}' and b.email = '{need_guide.GEMAIL}'
    CREATE (b) - [r:GUIDES] -> (a)
    """
    return command


def import_needs(df):
    needs = format_needs(df)
    with driver.session() as session:
        for i, need in needs.iterrows():
            command = update_need(need)
            session.write_transaction(run_command, command)
            command = connect_need_to_ord(need)
            session.write_transaction(run_command, command)


def format_need_guides(df):
    need_guides = df.parse("need guides")
    cols_to_drop = []
    for column in need_guides.columns:
        if column.startswith("Unnamed"):
            cols_to_drop.append(column)
    need_guides.drop(cols_to_drop, inplace=True, axis=1)
    need_guides.columns = ["NID", "NT", "GID", "GEMAIL", "GNAME"]
    return need_guides


def import_need_guides(df):
    need_guides = format_need_guides(df)
    with driver.session() as session:
        for i, need_guide in need_guides.iterrows():
            command = connect_guide_to_need(need_guide)
            session.write_transaction(run_command, command)


def format_responsibilities(df):
    responsibilities = df.parse("responsibilities")
    cols_to_drop = []
    for column in responsibilities.columns:
        if column.startswith("Unnamed"):
            cols_to_drop.append(column)
    responsibilities.drop(cols_to_drop, inplace=True, axis=1)
    renamed_columns = ["nid", "ntitle", "rid", "rtitle", "rdesc", "nid2", "ntitle2"]
    responsibilities.columns = renamed_columns
    responsibilities.drop(["nid2", "ntitle2"], axis=1, inplace=True)
    return responsibilities


def update_responsibility(responsibility):
    command = f"""
    MERGE (p:Responsibility {{nodeId: '{responsibility.rid}' }})
      ON CREATE SET p.created = timestamp()\n"""
    if responsibility.rdesc:
        command += add_on_create("description", responsibility.rdesc)
    if responsibility.rtitle:
        command += add_on_create("title", responsibility.rtitle)
    if responsibility.rdesc:
        command += add_on_match("description", responsibility.rdesc)
    if responsibility.rtitle:
        command += add_on_match("title", responsibility.rtitle)
    command += """
      RETURN p.title, p.nodeId
                 """
    return command


def connect_responsibility_to_need(resp):
    command = f"""MATCH (a:Responsibility), (b:Need) 
     WHERE a.nodeId = '{resp.rid}' and b.nodeId = '{resp.nid}'
     CREATE (a) - [r:FULFILLS] -> (b)
     """
    return command


def connect_responsibility_to_org(resp, orgId=ORG_ID):
    command = f"""MATCH (a:Org), (b:Responsibility)
WHERE a.orgId = '{orgId}' and b.nodeId = '{resp.rid}'
CREATE (a) - [r:HAS] -> (b)
    """
    return command


def import_responsibilities(df):
    responsibilities = format_responsibilities(df)
    # print(responsibilities.head())
    with driver.session() as session:
        for i, resp in responsibilities.iterrows():
            command = update_responsibility(resp)
            session.write_transaction(run_command, command)
            command = connect_responsibility_to_need(resp)
            session.write_transaction(run_command, command)
            command = connect_responsibility_to_org(resp)
            session.write_transaction(run_command, command)


def format_responsibilities_guides(df):
    responsibilities_guides = df.parse("responsibility guides")
    cols_to_drop = []
    for column in responsibilities_guides.columns:
        if column.startswith("Unnamed"):
            cols_to_drop.append(column)
    responsibilities_guides.drop(cols_to_drop, inplace=True, axis=1)
    renamed_columns = ["ntitle", "rid", "rtitle", "gid", "gemail", "gname"]
    responsibilities_guides.columns = renamed_columns
    return responsibilities_guides


def connect_guide_to_responsibility(need_guide):
    command = f"""MATCH (a:Responsibility), (b:Person) 
    WHERE a.nodeId = '{need_guide.rid}' and b.email = '{need_guide.gemail}'
    CREATE (b) - [r:GUIDES] -> (a)
    """
    return command


def import_responsibility_guides(df):
    rguides = format_responsibilities_guides(df)
    with driver.session() as session:
        for i, guide in rguides.iterrows():
            command = connect_guide_to_responsibility(guide)
            session.write_transaction(run_command, command)


import_users(df)
import_needs(df)
import_need_guides(df)
import_responsibilities(df)
import_responsibility_guides(df)
driver.close()

TODO:
Connect up realisers to Responsibilities
Connect up Responsibility dependencies
Add in links in both needs/reponsibilities
Stop it creating a new edge between nodes if the edge already exists - thats a pretty annoying thing when you are developing
