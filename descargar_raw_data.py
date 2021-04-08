import MySQLdb
import pandas as pd
import logging
import os
from sys import platform

global logger
global file_format

if platform.startswith('win'):
    ip_bd_edu = "26.2.206.141"
else:
    ip_bd_edu = "192.168.11.150"


def mantener_log():
    global logger
    global file_format
    logger = logging.getLogger(__name__)  # P: número de proceso, L: número de línea
    logger.setLevel(logging.DEBUG)  # deja pasar todos desde debug hasta critical
    print_handler = logging.StreamHandler()
    print_format = logging.Formatter('[{asctime:s}] {levelname:s} L{lineno:d}| {message:s}',
                                     '%Y-%m-%d %H:%M:%S', style='{')
    file_format = logging.Formatter('[{asctime:s}] {processName:s} P{process:d}@{name:s} ' +
                                    '${levelname:s} L{lineno:d}| {message:s}',
                                    '%Y-%m-%d %H:%M:%S', style='{')
    # printear desde debug hasta critical:
    print_handler.setLevel(logging.DEBUG)
    print_handler.setFormatter(print_format)
    logger.addHandler(print_handler)


def procesar_raw_data(cursor):
    datos = [row for row in cursor.fetchall() if row[0] is not None]
    df_ = pd.DataFrame(datos, columns=[i[0] for i in cursor.description])
    df_.set_index('id', inplace=True)
    
    columnas_valor = ['latitud', 'longitud']
    for columna in columnas_valor:
        if columna in df_.columns:
            try:
                df_[columna] = pd.to_numeric(df_[columna])
            except ValueError:
                logger.exception(f'Error en columna {columna}')
        else:
            logger.warning(f'Columna {columna} no está en estos datos')

    df_['fecha_hora_consulta'] = pd.to_datetime(df_['fecha_hora_consulta'], errors='raise',
                                                format="%Y-%m-%d %H:%M:%S")
    df_['fecha_evento'] = pd.to_datetime(df_['fecha_evento'], errors='raise',
                                         format="%Y-%m-%d")
    df_['fecha_hora_evento'] = df_['fecha_evento'] + df_['hora_evento']

    return df_


def consultar_raw_ttec_variable(fecha_dia):
    db1 = MySQLdb.connect(host=ip_bd_edu,
                          user="brunom",
                          passwd="Manzana",
                          db="tracktec",
                          charset='utf8')

    cur1 = db1.cursor()

    logger.info(f'Consultando electricos fecha {fecha_dia}')
    cur1.execute(f"""
                 SELECT * FROM tracktec.eventos
                 WHERE fecha_evento = '{fecha_dia}'
                 AND bus_tipo = 'Electric';
                 """
                 )

    logger.info(f'Guardando raw data electrica..')
    fecha_ = fecha_dia.replace('-', '_')
    df__ = procesar_raw_data(cur1)
    df__.to_parquet(f'raw_data_Ttec_{fecha_}.parquet', compression='gzip')

    cur1.close()
    db1.close()
    logger.info(f'Raw data electrica guardada..')
    return None


def consultar_raw_ttec_variable_diesel(fecha_dia):
    db1 = MySQLdb.connect(host=ip_bd_edu,
                          user="brunom",
                          passwd="Manzana",
                          db="tracktec",
                          charset='utf8')

    cur1 = db1.cursor()
    
    logger.info(f'Consultando diesel fecha {fecha_dia}')
    cur1.execute(f"""
                 SELECT * FROM tracktec.eventos
                 WHERE fecha_evento = '{fecha_dia}'
                 AND bus_tipo = 'Fuel'
                 """
                 )

    logger.info(f'Guardando raw data diesel..')
    fecha_ = fecha_dia.replace('-', '_')
    df__ = procesar_raw_data(cur1)
    df__.to_parquet(f'raw_data_Ttec_dsl_{fecha_}.parquet', compression='gzip')

    cur1.close()
    db1.close()
    logger.info(f'Raw data diesel guardada..')
    return None


def descargar_raw_semana_ttec_v2(fechas, reemplazar=False):
    for fecha_ in fechas:
        if reemplazar or not os.path.isfile(f'raw_data_Ttec_{fecha_}.parquet'):
            fecha__ = fecha_.replace('_', '-')
            consultar_raw_ttec_variable(fecha__)
        else:
            logger.info(f"No se va a reemplazar raw data Ttec electricos de fecha {fecha_}")
        if reemplazar or not os.path.isfile(f'raw_data_Ttec_dsl_{fecha_}.parquet'):
            fecha__ = fecha_.replace('_', '-')
            consultar_raw_ttec_variable_diesel(fecha__)
        else:
            logger.info(f"No se va a reemplazar raw data Ttec diesel de fecha {fecha_}")


def raw_pipeline(dia_ini, mes, anno, replace_data_ttec=False, sem_especial=[]):
    # dia_ini tiene que ser un día lunes
    # Sacar fechas de interes a partir de lunes inicio de semana
    fecha_dia_ini = pd.to_datetime(f'{dia_ini}-{mes}-{anno}', dayfirst=True).date()
    dia_de_la_semana = fecha_dia_ini.isoweekday()
    if dia_de_la_semana != 1:
        logger.error(f"Primer día no es lunes y se quiere ocupar parámetro sem_especial, "
                     f"numero dia_ini: {dia_de_la_semana}")
        exit()

    fechas_de_interes = []
    if not sem_especial:
        for i in range(0, 7):
            fechas_de_interes.append(fecha_dia_ini + pd.Timedelta(days=i))
    else:
        # se buscan días de la semana entre fecha inicio y el domingo
        if len(sem_especial) != len(set(sem_especial)):
            logger.error(f"Semana especial no debe repetir números: {sem_especial}")
            exit()
        for i in sem_especial:
            if 0 < i < 8:
                fechas_de_interes.append(fecha_dia_ini + pd.Timedelta(days=(i - 1)))
            else:
                logger.error(f"Semana especial debe ser lista con números 1 al 7: {sem_especial}")
                exit()
    fechas_de_interes = [x.strftime('%Y-%m-%d') for x in fechas_de_interes]

    logger.info(f'Semana de interes: {fechas_de_interes}')

    nombre_semana = f"raw_semana_{fechas_de_interes[0].replace('-', '_')}"

    # buscar si ya existia carpeta
    if not os.path.isdir(nombre_semana):
        logger.info(f'Creando carpeta {nombre_semana}')
        os.mkdir(nombre_semana)

    os.chdir(nombre_semana)

    # Crear variable que escribe en log file de este dia
    file_handler = logging.FileHandler(f'{nombre_semana}.log')

    # no deja pasar los debug, solo info hasta critical
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(file_format)
    logger.addHandler(file_handler)

    fechas_de_interes = [x.replace('-', '_') for x in fechas_de_interes]

    logger.info('Consultando servidor mysql por datos tracktec')
    if replace_data_ttec:
        logger.info("Como replace_data_ttec=True se van a reemplazar archivos parquet")
    descargar_raw_semana_ttec_v2(fechas_de_interes, replace_data_ttec)

    logger.info('Listo todo para esta semana')
    os.chdir('..')


if __name__ == '__main__':
    mantener_log()
    # estas filas son para debug
    # os.chdir('data')
    # consultar_ttec_variable_diesel('2020-08-31')
    # consultar_ttec_variable_diesel('2020-11-19')
    # consultar_ttec_variable_diesel('2021-02-23')
    # exit()
    reemplazar_data_ttec = False
    raw_pipeline(2, 11, 2020, reemplazar_data_ttec)
    raw_pipeline(9, 11, 2020, reemplazar_data_ttec)
    raw_pipeline(16, 11, 2020, reemplazar_data_ttec, sem_especial=[1, 2, 3, 4, 5])
    logger.info('Listo todo')
