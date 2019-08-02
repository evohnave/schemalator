# -*- coding: utf-8 -*-
"""

@author: EA Vanhove

"""

# To do: turn into a class


# For testing
#   create variable:
#     theSchema = <paste in output from get_schema(OUT_BUCKET)
#     theSchema_jsonValue = <paste in the output from get_schema().jsonValue()
#   to test, run print(parse_struct_fields(theSchema_jsonValue['fields']))
#   to test only a single field, copy that field into a variable and run
#     print(parse_field(aField))

def parse_schema(schema):
    """
    schema == pyspark.sql.types.StructType from a schema
    output == redshift external table schema format
    """
    redshift = parse_struct_fields(schema.jsonValue()['fields'], top_level=True)
    redshift = redshift[len('struct<'):-2]
    redshift = '(\n' + redshift + ')\n'
    return redshift

def parse_struct_fields(fields, top_level=False):
    """
    parses the fields of a schema struct
      use top_level=True for only the initial entry from parse_schema
    return format:
        struct<Data>
    """
    sep = ':\t'[top_level]
    output = 'struct<'
    for field in fields:
        output += parse_field(field, sep)
    output = output[:-2] + '>'
    return output

def parse_field(field, sep='\t'):
    """
    parse a field from a schema
      pass in sep=':' if in a struct below the top level
    """
    field_name = field['name']
    field_type = field['type']
    try:
        if field_type not in ('string', 'long'):
            if field_type['type'] == 'struct':
                field_type = parse_struct_fields(field['type']['fields'])
            elif field_type['type'] == 'array':
                field_type = parse_array_fields(field['type']['elementType'])
            else:
                field_type = 'Need To Raise Error!!!'
                #type == 'other'
        else:
            if field_type == 'string':
                field_type = 'varchar'
            else:
                field_type = 'bigint'
    except KeyError:
        pass # To Do
    return f"{field_name}{sep}{field_type},\n"

#pylint: disable=C0103
#  Catches elementType not snake_case
#  elementType is a pyspark.sql.type
def parse_array_fields(elementType):
    """
    parses the fields of a schema array
    return format:
        array<data>
    """
    insides = 'TO DO: parse_array_fields'
    if isinstance(elementType, dict):
        if elementType['type'] == 'struct':
            insides = parse_struct_fields(elementType['fields'])
        elif elementType['type'] == 'array':
            insides = parse_array_fields(elementType['fields'])
        else:
            insides = 'Need To Raise Error!!!'
    else:
        insides = elementType['type']  # haven't seen yet...
    return f"array<{insides}>"
