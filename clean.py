import sqlite3
import pandas as pd
import numpy as np
import fastparquet
import xlrd
import xlsxwriter
import openpy
pd.options.mode.chained_assignment = None  # default='warn'


def conection_sqlite():
    conn = sqlite3.connect(":memory:")  # aca se indica el nombre de la db.
    cur = conn.cursor()
    return conn, cur


def read_data_parquet(in_file):
    data = pd.read_parquet(in_file)
    return data


def read_data_xlsx(in_file):
    data = pd.read_excel(in_file, header=0, dtype=str)
    return data


def create_table_in_sqlite(data, conn, name_table):
    data.to_sql(
        name=name_table,
        con=conn,
        if_exists="replace",
    )


def query_tables(conn, table_name):
    # esto se puede industrializar para que en la orquestación genere la consulta que se considere pertienente
    query = 'SELECT * FROM {}'.format(table_name)
    df_table = pd.read_sql(query, con=conn)
    return df_table


def get_merge_original_data_reps(list_tables, list_of_list_atributtes_delete, keys_merge, atributtes_merge):
    iteration = 0
    # ________________eliminación atributos____________________________
    for dataframe in list_tables:
        for atributte in list_of_list_atributtes_delete[iteration]:
            dataframe.pop(atributte)
        iteration += 1
    # __________________unión de fuentes reps____________________________
    data = pd.merge(list_tables[0], list_tables[1][atributtes_merge + [
                    keys_merge[1]]], left_on=keys_merge[0], right_on=keys_merge[1], how='left')
    return data


def filter_data(data, list_atributtes_filter, list_conditionals_filter):
    iterator = 0
    for atributte in list_atributtes_filter:
        for conditional in list_conditionals_filter[iterator]:
            data = data[data[atributte] != conditional]
        iterator += 1
    return data


def clean_characters_special(data, specials_characters, correct_specials_characters, list_atributtes_correct):
    for atributte in list_atributtes_correct:
        iterator_character = 0
        for character in specials_characters:
            if character != specials_characters[-1]:
                data[atributte] = data[atributte].str.upper().apply(lambda x: x.replace(
                    character, correct_specials_characters[iterator_character]).strip())
                iterator_character += 1
            else:
                data[atributte] = data[atributte].str.upper().apply(lambda x: x.replace(
                    character, correct_specials_characters[iterator_character]).strip())
                iterator_character = 0
    return data


def comparative_original_reps_hospitales(data_reps_original, data_hospitals, column_reps, column_hospitals):
    hospitals = data_hospitals.copy()
    reps_original = data_reps_original.copy()[[column_reps]]
    reps_original['data_contain'] = reps_original[column_reps].apply(
        lambda x: x.split(' ', maxsplit=len(x.split(' '))-2)[-1])
    reps_original_list_data_contain = list(
        reps_original['data_contain'].drop_duplicates())
    iterator = 0

    for data_contain in reps_original_list_data_contain:
        try:
            if iterator != 0:
                hospitals['bool'] = np.where(hospitals[column_hospitals].str.contains(
                    data_contain) == True, True, hospitals['bool'])

            else:
                hospitals['bool'] = np.where(
                    hospitals[column_hospitals].str.contains(data_contain) == True, True, False)
            iterator += 1
        except:
            print(data_contain)
    hospitals_missing_in_original_reps = hospitals[hospitals['bool'] == False]
    return hospitals_missing_in_original_reps


def execute():
    # _______________________configuración 'entradas y salidas'_____________________
    conn, cur = conection_sqlite()
    provider_input = 'data_lake/landing_prestadores/providers.xlsx'
    capacity_input = 'data_lake/landing_capacidad/capacity.xlsx'
    hospital_input = 'data_lake/landing_hospitals/hospitals_migration.parquet'
    # ________________________read data_____________________________________________
    provider = read_data_xlsx(provider_input)
    capacity = read_data_xlsx(capacity_input)
    # Se carga esta data a SQLITE para simular procesamiento desde diferentes fuentes
    hospitals_original_data = read_data_parquet(hospital_input)

    # ________________________crear tabla hospitales en sqlite ______________________
    create_table_in_sqlite(hospitals_original_data, conn, 'hospitales')
    # _________________________leer la tabla hospitales del motor_____________________
    hospitals_migration = query_tables(conn, 'hospitales')

    # ______________________Hacer merge entre las datas de capacidad y prestadores________
    keys_merge = ['codigo_habilitacion', 'codigo_habilitacion']
    list_of_list_atributtes_delete = [
        ['habi_codigo_habilitacion'], ['tido_codigo']]
    atributtes_merge = ['razon_social', 'fax', 'gerente', 'fecha_radicacion',
                        'fecha_vencimiento', 'fecha_cierre', 'telefono_adicional', 'email_adicional', 'rep_legal']
    original_reps = get_merge_original_data_reps(
        [capacity, provider], list_of_list_atributtes_delete, keys_merge, atributtes_merge)

    # __________________________________limpieza_____________________________________
    # ----------> Filtros
    list_atributtes_filter = ['clpr_nombre']
    list_conditionals_filter = [['Profesional Independiente']]
    original_reps_clean = filter_data(
        original_reps, list_atributtes_filter, list_conditionals_filter)
    # --------> corrección de caracteres especiales y mayusculas
    specials_characters = ['Á', 'É', 'Í', 'Ó', 'Ú',
                           'À', 'È', 'Ì', 'Ò', 'Ù', '  ', '(', ')']
    correct_specials_characters = [
        'A', 'E', 'I', 'O', 'U', 'A', 'E', 'I', 'O', 'U', ' ', '', '']
    hospitals_list_atributtes_correct = ['hospital']
    original_reps_list_atributtes_correct = ['sede_nombre']
    hospitals_migration_clean = clean_characters_special(
        hospitals_migration, specials_characters, correct_specials_characters, hospitals_list_atributtes_correct)
    original_reps_clean = clean_characters_special(
        original_reps_clean, specials_characters, correct_specials_characters, original_reps_list_atributtes_correct)
    # _______________________Hospitales que no se encuentran en reps_original pero si en hospitals_migration_________
    hospitals_missing_in_original_reps = comparative_original_reps_hospitales(
        original_reps_clean, hospitals_migration_clean, 'sede_nombre', 'hospital')
    hospitals_missing_in_original_reps.to_csv(
        'data_lake/reports/hospitals_missing_in_original_reps.csv')
    original_reps_clean.to_csv('data_lake/reports/original_reps_clean.csv')
    hospitals_migration_clean.to_csv(
        'data_lake/reports/hospitals_migration_clean.csv')


if __name__ == '__main__':
    execute()
