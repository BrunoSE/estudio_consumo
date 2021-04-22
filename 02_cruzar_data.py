import pandas as pd
import logging
import os
from datetime import timedelta
from sys import platform

global logger
global file_format
global df
global df_f

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


def procesar_dia(fecha):
    fecha_pd = pd.to_datetime(fecha.replace('_', '-'))
    global df
    global df_f
    dfx = pd.read_parquet(f'data_Ttec_dsl_{fecha}.parquet')
    dfx.sort_values(by=['fecha_hora_evento'], inplace=True)
    df_dia = pd.merge_asof(df.loc[df['Fecha'] == fecha_pd], dfx,
                           left_on='Hora_en_parada',
                           right_on='fecha_hora_evento',
                           left_by='Patente', right_by='patente',
                           suffixes=['', '_Ttec'],
                           tolerance=timedelta(seconds=5),
                           direction='nearest')

    logger.info(f'Datos Tracktec {fecha}: {len(dfx.index)}')
    logger.info(f'Datos ADATRAP {fecha}: {len(df_dia.index)}')
    logger.info(f'Datos cruzados unicos: {len(df_dia.loc[~df_dia["evento_id_consc"].isna()].index)}')
    
    df_f.append(df_dia.copy())
    return None


def pipeline(dia_ini, mes, anno, sem_especial=[]):
    # dia_ini tiene que ser un día lunes
    # Sacar fechas de interes a partir de lunes inicio de semana
    fecha_dia_ini = pd.to_datetime(f'{dia_ini}-{mes}-{anno}', dayfirst=True).date()
    dia_de_la_semana = fecha_dia_ini.isoweekday()
    if dia_de_la_semana != 1:
        logger.error(f"Primer día no es lunes (ocupar parámetro sem_especial), "
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
        logger.warning(f'No existe carpeta {nombre_semana}')
        exit()

    os.chdir(nombre_semana)

    # Crear variable que escribe en log file de este dia
    file_handler = logging.FileHandler(f'{nombre_semana}.log')

    # no deja pasar los debug, solo info hasta critical
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(file_format)
    logger.addHandler(file_handler)

    fechas_de_interes = [x.replace('-', '_') for x in fechas_de_interes]

    logger.info('Cruzando la data')

    for fecha_ in fechas_de_interes:
        procesar_dia(fecha_)

    logger.info('Listo todo para esta semana')
    os.chdir('..')


if __name__ == '__main__':
    global df
    global df_f
    df_f = []
    mantener_log()

    df = pd.read_csv('data/adatrap1/Perfil.csv')
    logger.info(f'Datos ADATRAP: {len(df.index)}')
    df = df.loc[df['Hora_en_parada'] != '0']
    df['Hora_en_parada'] = pd.to_datetime(df['Hora_en_parada'])
    df.sort_values(by=['Hora_en_parada'], inplace=True)
    df['Fecha'] = df['Hora_en_parada'].dt.date

    # estas filas son para debug
    # os.chdir('data')
    # consultar_ttec_variable_diesel('2020-08-31')
    # consultar_ttec_variable_diesel('2020-11-19')
    # consultar_ttec_variable_diesel('2021-02-23')
    # exit()
    pipeline(2, 11, 2020, sem_especial=[2, 3, 4, 5, 6, 7])
    pipeline(9, 11, 2020, sem_especial=[1, 2, 3, 4, 6, 7])
    pipeline(16, 11, 2020, sem_especial=[1, 2, 3, 4, 5])

    df_f = pd.concat(df_f)
    logger.info(f'Datos ADATRAP con Hora_parada: {len(df.index)}')
    logger.info(f'Datos del cruce: {len(df_f.index)}')
    logger.info(f'Datos del cruce con data de Consumo: {len(df_f.loc[~df_f["evento_id_consc"].isna()].index)}')
    df_f['dT_merge'] = abs((df_f['Hora_en_parada'] -
                            df_f['fecha_hora_evento']) / pd.Timedelta(seconds=1))

    df_f.sort_values(by=['patente', 'fecha_hora_evento', 'dT_merge'], inplace=True)
    df_f.drop_duplicates(subset=['patente', 'fecha_hora_evento'], keep='first', inplace=True)
    df_f.sort_values(by=['Patente', 'Hora_en_parada'], inplace=True)

    logger.info(f'Datos del cruce unicos: {len(df_f.index)}')
    logger.info(f'Datos del cruce unicos con data de Consumo: {len(df_f.loc[~df_f["evento_id_consc"].isna()].index)}')
    df_f = df_f.loc[~df_f["evento_id_consc"].isna()]
    logger.info('Guardando cruce')
    df_f['Fecha'] = pd.to_datetime(df_f['Fecha'])
    df_f.to_parquet(f'cruce_data_104i_v1.parquet', compression='gzip')
    logger.info('Listo todo')
