#! /usr/bin/env python3

from pyspark.sql.functions import struct
from pyspark.sql.types import StructField, StructType

def get_path(path, name):
    return '.'.join([path] + [name])

def add_subtree(path, value):

    if len(path) == 0:
        return value

    name, *path = path
    if len(path) == 0:
        return value.alias(name)

    return struct(add_subtree(path, value)).alias(name)

def create_tree(dataframe, path, struct_field, new_path, value):

    name, *new_path = new_path
    if isinstance(struct_field, StructField):
        if len(new_path) > 0:
            return struct(add_subtree(new_path, value)).alias(name)
        return add_subtree(new_path, value).alias(name)

    field_found = False

    fields = []
    for field in struct_field.fields:
        if field.name == name:
            field_found = True
            if len(new_path) == 0:
                fields.append(value.alias(name))

            else:
                field_value = field.dataType
                if isinstance(field_value, StructType) and len(new_path) > 0:
                    path_to_field = get_path(path, field.name)
                    fields.append(create_tree(
                        dataframe,
                        path_to_field,
                        field_value,
                        new_path,
                        value
                    ).alias(name))

                else:
                    if len(new_path) > 0:
                        fields.append(
                            struct(add_subtree(new_path, value)).alias(name)
                        )
                    else:
                        fields.append(
                            add_subtree(new_path, value).alias(name)
                        )
        else:
            path_to_field = get_path(path, field.name)
            fields.append(dataframe[path_to_field])

    if not field_found:
        fields.append(
            add_subtree(new_path, value).alias(name)
        )

    return struct(*fields)

def get_updated_dataframe(dataframe, path, value):

    schema = dataframe.schema
    name, *path = path.split(sep='.')

    for field in schema.fields:
        if name == field.name:
            if len(path) > 0:
                coloums = create_tree(dataframe, name, field.dataType, path, value)
                break
            coloums = add_subtree(path, value)
    else:
        if len(path) > 0:
            coloums = struct(add_subtree(path, value)).alias(name)
        else:
            coloums = add_subtree(path, value)
    return dataframe.withColumn(name, coloums)

def update_df(df, columns_dict):
    """
        Updates existing columns or creates new in dataframe df using
        columns from columns_dict.
        :param df: input dataframe
        :type df: pyspark.sql.Dataframe
        :param columns_dict: Key-value dictionary of columns which need to
        be updated. Key is a column name in
        the format of path.to.col
        :type param: Dict[str, pyspark.sql.Column]
        :return: dataframe with updated columns
        :rtype pyspark.sql.DataFrame
    """
    updated_df = df
    for key in columns_dict.keys():
        updated_df = get_updated_dataframe(updated_df, key, columns_dict[key])

    return updated_df
