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


def procesar_datos_consulta_v2(cursor, columnas_adicionales=[]):
    datos = [row for row in cursor.fetchall() if row[0] is not None]
    df_ = pd.DataFrame(datos, columns=[i[0] for i in cursor.description])
    df_.set_index('id', inplace=True)
    columnas_valor = ['latitud', 'longitud', 'valor_soc']
    for col_a in columnas_adicionales:
        columnas_valor.append(col_a)
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


def consultar_ttec_variable(fecha_dia):
    db1 = MySQLdb.connect(host=ip_bd_edu,
                          user="brunom",
                          passwd="Manzana",
                          db="tracktec",
                          charset='utf8')

    cur1 = db1.cursor()
    variable1 = 'Temp Interior'
    abv1 = 'tint'
    variable2 = 'Temp Exterior'
    abv2 = 'text'
    variable3 = 'Temperatura de Motor'
    abv3 = 'tmot'
    variable4 = 'Potencia Total Generada'
    abv4 = 'ptg'
    variable5 = 'Potencia Total Consumida'
    abv5 = 'ptc'
    variable6 = 'Odómetro'
    abv6 = 'odom'

    columnas_var = [f'valor_{x}' for x in [abv1, abv2, abv3, abv4, abv5, abv6]]
    logger.info(f'Consultando debug con vars {columnas_var}')
    cur1.execute(f"""
                 SELECT * FROM
                 (
                     SELECT * FROM
                     (
                         SELECT * FROM
                         (
                 SELECT * FROM
                 (
                     SELECT * FROM
                     (
                         SELECT * FROM
                         (
                             SELECT * FROM
                             (
                                 SELECT * FROM
                                 (
                                     SELECT * FROM
                                     tracktec.eventos
                                     WHERE fecha_evento = '{fecha_dia}'
                                     AND hora_evento IS NOT NULL AND bus_tipo = 'Electric'
                                     AND PATENTE IS NOT NULL AND NOT (patente REGEXP '^[0-9]+')
                                 ) TABLEORIGINAL
                                 LEFT JOIN
                                     (SELECT valor AS valor_soc,
                                     evento_id AS evento_id_soc FROM
                                     tracktec.telemetria_
                                     WHERE (nombre = 'SOC' AND
                                            valor REGEXP '^[\\-]?[0-9]+\\.?[0-9]*$')
                                 ) AS t_soc
                                 ON TABLEORIGINAL.id=t_soc.evento_id_soc
                             ) TABLESOC
                             LEFT JOIN
                                 (SELECT valor AS valor_{abv1},
                                 evento_id AS evento_id_{abv1} FROM
                                 tracktec.telemetria_
                                 WHERE (nombre = '{variable1}' AND
                                        valor REGEXP '^[\\-]?[0-9]+\\.?[0-9]*$')
                                 ) AS t{abv1}
                             ON TABLESOC.id=t{abv1}.evento_id_{abv1}
                         ) TABLE1
                         LEFT JOIN
                             (SELECT valor AS valor_{abv2},
                             evento_id AS evento_id_{abv2} FROM
                             tracktec.telemetria_
                             WHERE (nombre = '{variable2}' AND
                                    valor REGEXP '^[\\-]?[0-9]+\\.?[0-9]*$')
                             ) AS t{abv2}
                         ON TABLE1.id=t{abv2}.evento_id_{abv2}
                     ) TABLE2
                     LEFT JOIN
                         (SELECT valor AS valor_{abv3},
                         evento_id AS evento_id_{abv3} FROM
                         tracktec.telemetria_
                         WHERE (nombre = '{variable3}' AND
                                valor REGEXP '^[\\-]?[0-9]+\\.?[0-9]*$')
                     ) AS t{abv3}
                     ON TABLE2.id=t{abv3}.evento_id_{abv3}
                 ) AS TABLE3
                             LEFT JOIN
                                 (SELECT valor AS valor_{abv4},
                                 evento_id AS evento_id_{abv4} FROM
                                 tracktec.telemetria_
                                 WHERE (nombre = '{variable4}' AND
                                        valor REGEXP '^[\\-]?[0-9]+\\.?[0-9]*$')
                                 ) AS t{abv4}
                             ON TABLE3.id=t{abv4}.evento_id_{abv4}
                         ) TABLE4
                         LEFT JOIN
                             (SELECT valor AS valor_{abv5},
                             evento_id AS evento_id_{abv5} FROM
                             tracktec.telemetria_
                             WHERE (nombre = '{variable5}' AND
                                    valor REGEXP '^[\\-]?[0-9]+\\.?[0-9]*$')
                             ) AS t{abv5}
                         ON TABLE4.id=t{abv5}.evento_id_{abv5}
                     ) TABLE5
                     LEFT JOIN
                         (SELECT valor AS valor_{abv6},
                         evento_id AS evento_id_{abv6} FROM
                         tracktec.telemetria_
                         WHERE (nombre = '{variable6}' AND
                                valor REGEXP '^[\\-]?[0-9]+\\.?[0-9]*$')
                     ) AS t{abv6}
                     ON TABLE5.id=t{abv6}.evento_id_{abv6}
                 ) AS TABLE6
                 WHERE
                 valor_soc IS NOT NULL
                 ORDER BY patente;
                 """
                 )

    logger.info(f'Procesando data debug..')
    df__ = procesar_datos_consulta_v2(cur1, columnas_var)
    fecha_ = fecha_dia.replace('-', '_')
    logger.info(f'Guardando data debug..')
    df__.to_parquet(f'data/data_Ttec_{fecha_}.parquet', compression='gzip')

    cur1.close()
    db1.close()
    logger.info(f'Listo debug..')
    return None


def consultar_transmisiones_tracktec_por_dia(fecha_dia):
    db1 = MySQLdb.connect(host=ip_bd_edu,
                          user="brunom",
                          passwd="Manzana",
                          db="tracktec",
                          charset='utf8')

    cur1 = db1.cursor()

    cur1.execute(f"""
                 SELECT * FROM
                 (
                     SELECT * FROM
                     (
                         SELECT * FROM
                         (
                             SELECT * FROM
                             (
                                 SELECT * FROM
                                 (
                                     SELECT * FROM
                                     tracktec.eventos
                                     WHERE fecha_evento = '{fecha_dia}'
                                     AND hora_evento IS NOT NULL AND bus_tipo = 'Electric'
                                     AND PATENTE IS NOT NULL AND NOT (patente REGEXP '^[0-9]+')
                                 ) TABLE1
                                 LEFT JOIN
                                     (SELECT valor AS valor_soc,
                                     evento_id AS evento_id_soc FROM
                                     tracktec.telemetria_
                                     WHERE (nombre = 'SOC' AND
                                            valor REGEXP '^[\\-]?[0-9]+\\.?[0-9]*$')
                                 ) AS t_soc
                                 ON TABLE1.id=t_soc.evento_id_soc
                             ) TABLE2
                             LEFT JOIN
                                 (SELECT valor AS valor_ptg,
                                 evento_id AS evento_id_ptg FROM
                                 tracktec.telemetria_
                                 WHERE (nombre = 'Potencia Total Generada' AND
                                        valor REGEXP '^[\\-]?[0-9]+\\.?[0-9]*$')
                                 ) AS t_ptg
                             ON TABLE2.id=t_ptg.evento_id_ptg
                         ) TABLE3
                         LEFT JOIN
                             (SELECT valor AS valor_ptc,
                             evento_id AS evento_id_ptc FROM
                             tracktec.telemetria_
                             WHERE (nombre = 'Potencia Total Consumida' AND
                                    valor REGEXP '^[\\-]?[0-9]+\\.?[0-9]*$')
                             ) AS t_ptc
                         ON TABLE3.id=t_ptc.evento_id_ptc
                     ) TABLE4
                     LEFT JOIN
                         (SELECT valor AS valor_odom,
                         evento_id AS evento_id_odo FROM
                         tracktec.telemetria_
                         WHERE (nombre = 'Odómetro' AND
                                valor REGEXP '^[\\-]?[0-9]+\\.?[0-9]*$')
                     ) AS t_odo
                     ON TABLE4.id=t_odo.evento_id_odo
                 ) AS TABLE5
                 WHERE
                 valor_soc IS NOT NULL
                 ORDER BY patente;
                 """
                 )

    df__ = procesar_datos_consulta(cur1)

    cur1.close()
    db1.close()

    return df__


def descargar_semana_ttec(fechas, reemplazar=True):
    for fecha_ in fechas:
        if reemplazar or not os.path.isfile(f'data_Ttec_{fecha_}.parquet'):
            fecha__ = fecha_.replace('_', '-')
            logger.info(f"Descargando data Tracktec para fecha {fecha_}")
            dfx = consultar_transmisiones_tracktec_por_dia(fecha__)
            dfx.to_parquet(f'data_Ttec_{fecha_}.parquet', compression='gzip')
        else:
            logger.info(f"No se va a reemplazar data Ttec de fecha {fecha_}")


def pipeline(dia_ini, mes, anno, replace_data_ttec=False, sem_especial=[]):
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

    nombre_semana = f"semana_{fechas_de_interes[0].replace('-', '_')}"

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
    descargar_semana_ttec(fechas_de_interes, replace_data_ttec)

    logger.info('Listo todo para esta semana')
    os.chdir('..')


if __name__ == '__main__':
    mantener_log()
    consultar_ttec_variable('2020-11-19')
    exit()
    reemplazar_data_ttec = False
    pipeline(2, 11, 2020, reemplazar_data_ttec)
    pipeline(9, 11, 2020, reemplazar_data_ttec)
    pipeline(16, 11, 2020, reemplazar_data_ttec)
    logger.info('Listo todo')
