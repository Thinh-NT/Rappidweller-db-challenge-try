import jaydebeapi

from helper.benerator_generator import generate_node

# THIS PART IS TO CONNECT TO DATABASE
# Database configs
postgresql_driver = 'org.postgresql.Driver'
postgresql_path = 'lib/postgresql-42.3.5.jar'

postgresql_database = 'benerator'
postgresql_port = 35432
postgresql_url = f'jdbc:postgresql://0.0.0.0:{postgresql_port}/{postgresql_database}'

# Authenticate
postgresql_username = 'benerator'
postgresql_password = 'benerator'

conn = jaydebeapi.connect(
    postgresql_driver,
    postgresql_url,
    [postgresql_username, postgresql_password],
    postgresql_path
)


# START GETTING INFORMATION
cursor = conn.cursor()

# Get list of table
method = conn.jconn.getMetaData().getTables(None, "public", "%", ["TABLE"])
cursor._rs = method
cursor._meta = method.getMetaData()
read_results = cursor.fetchall()
table_list = [row[2] for row in read_results]

# Get constrain key, this step will get all the information to generate references in benerator node
reference_dict = {}
for table in table_list:
    # This will get the constraint keys base on table name
    method = conn.jconn.getMetaData().getImportedKeys(None, None, table)
    cursor._rs = method
    cursor._meta = method.getMetaData()
    read_results = cursor.fetchall()
    # THe index of information base on row will be hard code for now, it maybe add more mapping
    reference_dict[table] = {
        row[2]: {'name': row[11], 'from': row[7]} for row in read_results}


# Get order to generate XML node for references table first
# This set doesn not have any references or reference it self so it can be everywhere.
fronts = set()
tables_order = []  # This list keep the order of tables so reference tables can appear first
for key, refs in reference_dict.items():
    # This will be in the front
    if (len(refs) == 1 and key in refs) or not refs:
        fronts.add(key)
    else:
        to_appends = [ref for ref in refs if ref not in tables_order]
        if key in tables_order:
            # Replace old table with their references
            _key_index = tables_order.index(key)
            tables_order = tables_order[:_key_index] + \
                to_appends + tables_order[_key_index:]
        else:
            tables_order += to_appends
            tables_order.append(key)

# Add remain tables, anywhere can work, don't know why I use index 0
for front_table in fronts:
    if front_table not in tables_order:
        tables_order.insert(0, front_table)


# Prepare enough information to write benerator nodes
tables_infomation = {}
for table in tables_order:
    # Init table
    tables_infomation[table] = {}
    # It will get all meta data for columns for table
    method = conn.jconn.getMetaData().getColumns(None, None, table, None)
    cursor._rs = method
    cursor._meta = method.getMetaData()
    read_results = cursor.fetchall()
    for row in read_results:
        tables_infomation[table][row[3]] = {
            'node_type': 'attribute',
            'type': row[5].upper()
        }

    # Update primary fields
    method = conn.jconn.getMetaData().getPrimaryKeys(None, None, table)
    cursor._rs = method
    cursor._meta = method.getMetaData()
    read_results = cursor.fetchall()
    for row in read_results:
        # The primary fields already available, but need to update to be pk
        tables_infomation[table][row[3]].update({
            'node_type': 'id',
        })

    # Get information from references
    for reference, reference_info in reference_dict[table].items():
        tables_infomation[table][reference_info['name']] = {
            'node_type': 'reference',
            'from': reference_info['from'],
            'reference_table': reference
        }

generate_node(tables_information=tables_infomation, output_url='result.xml')
