from typing import Dict
import os
from .mapping_src import type_map

'''
SAMPLE NODE
<generate type="table_y">
    <id name="primarykey" type="integer"/> # if primary key -> id instead attribute
    <attribute name="column1" type="string"/>
    <attribute name="column2" type="big_decimal"/>
    <attribute name="column3" type="short"/>
    <reference name="tablex_id" selector="select primarykey from tablex" distribution="random"/> # reference because column tablex_id is foreign key column from table x
</generate>
'''

generate_node_begin = '''<generate type="{table_name}">'''
generate_node_end = '''</generate>'''
reference_p = '''    <reference name="{column_name}" selector="select {_from} from {reference}" distribution="random"/>'''
id_attribute_p = '''    <{node_type} name="{column_name}" type="{column_type}"/>'''


def get_node_type(column_name, node_type: str) -> str:
    """Try to predict the node type with simple logic

    Args:
        column_name (str): input to predict
        node_type (str): default node_type

    Returns:
        str: type of node
    """
    if any(word in column_name.lower() for word in ['pwd', 'password']):
        return 'hash'
    if any(word in column_name.lower() for word in ['name', 'surname', 'address']):
        return 'encrypt'
    if any(word in column_name.lower() for word in ['credit', 'card number']):
        return 'mask'
    return node_type


def generate_node(tables_information: Dict, output_url: str) -> None:
    """Generate node from tables information

    Args:
        tables_information (Dict[str, Dict])
        output_url (str)
    """
    _file = open(output_url, "w")

    for name, info in tables_information.items():
        nodes = []
        nodes.append(generate_node_begin.format(table_name=name))
        for column_name, column_info in info.items():
            if column_info['node_type'] == 'reference':
                nodes.append(
                    reference_p.format(
                        column_name=column_name,
                        _from=column_info['from'], reference=column_info['reference_table']))
            else:
                node_type = get_node_type(column_name, column_info['node_type'])
                nodes.append(id_attribute_p.format(
                    node_type=node_type, column_name=column_name, column_type=type_map[column_info['type']]))
        nodes.append(generate_node_end)

        for node in nodes:
            _file.write(node + '\n')
        _file.write('\n')
    _file.close()
