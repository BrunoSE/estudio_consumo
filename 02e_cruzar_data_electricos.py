import pandas as pd
import numpy as np
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

columnas_utiles_adatrap = ['Servicio_transantiago', 'Servicio_usuario', 'Patente',
                           'Código_parada_transantiago', 'Código_parada_usuario',
                           'Nombre_parada', 'Hora_inicio_expedición', 'Hora_fin_expedición',
                           'Cumplimiento', 'Secuencia_parada', 'Identificador_expedición_día',
                           'Distancia_parada_desde_inicio_ruta', 'Subidas_expandidas',
                           'Bajadas_expandidas', 'Perfil_carga_al_llegar', 'Capacidad_bus',
                           'Hora_en_parada', 'Periodo_transantiago_inicio_expedicion',
                           'Periodo_transantiago_parada_expedición', 'Tipo_dia',
                           'Zona_paga', 'Número_transacciones_en_parada',
                           'Media_hora_de_inicio_expedición', 'Media_hora_en_parada', 'Fecha']

columnas_utiles_ttec_ele = ['patente', 'bus_tipo', 'geozona',
                            'valor_soc', 'valor_tint', 'valor_text',
                            'valor_tmot', 'valor_ptg', 'valor_ptc',
                            'valor_odom', 'fecha_hora_evento']

columnas_utiles_ele = columnas_utiles_adatrap + columnas_utiles_ttec_ele
dict_col_ttec_back_ele = {}
dict_col_ttec_forw_ele = {}
for i in range(len(columnas_utiles_ttec_ele)):
    dict_col_ttec_back_ele[columnas_utiles_ttec_ele[i]] = f'{columnas_utiles_ttec_ele[i]}_back'
    dict_col_ttec_forw_ele[columnas_utiles_ttec_ele[i]] = f'{columnas_utiles_ttec_ele[i]}_forw'

col_forw_ele = list(dict_col_ttec_forw_ele.values())

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


def procesar_dia_ele(fecha):
    fecha_pd = pd.to_datetime(fecha.replace('_', '-'))
    global df
    global df_f
    dfx = pd.read_parquet(f'data_Ttec_{fecha}.parquet')
    dfx.sort_values(by=['fecha_hora_evento'], inplace=True)

    df_back = pd.merge_asof(df.loc[df['Fecha'] == fecha_pd], dfx,
                          left_on='Hora_en_parada',
                          right_on='fecha_hora_evento',
                          left_by='Patente', right_by='patente',
                          suffixes=['', '_Ttec'],
                          tolerance=timedelta(seconds=240),
                          direction='backward')
    
    logger.info(f' - - FECHA :  {fecha}')
    logger.info(f'Datos ADATRAP en la fecha: {len(df.loc[df["Fecha"] == fecha_pd].index)}')
    logger.info(f'Datos Tracktec en la fecha: {len(dfx.index)}')
    logger.info(f'Datos cruzados backw: {len(df_back.loc[~df_back["evento_id_soc"].isna()].index)}')

    df_back['dT_back'] = abs((df_back['Hora_en_parada'] -
                            df_back['fecha_hora_evento']) / pd.Timedelta(seconds=1))

    df_back.sort_values(by=['Patente', 'Hora_en_parada'], inplace=True)
    df_back = df_back.loc[~df_back["evento_id_soc"].isna()]


    df_forw = pd.merge_asof(df.loc[df['Fecha'] == fecha_pd], dfx,
                          left_on='Hora_en_parada',
                          right_on='fecha_hora_evento',
                          left_by='Patente', right_by='patente',
                          suffixes=['', '_Ttec'],
                          tolerance=timedelta(seconds=240),
                          direction='forward')

    logger.info(f'Datos cruzados forw: {len(df_forw.loc[~df_forw["evento_id_soc"].isna()].index)}')

    df_forw['dT_forw'] = abs((df_forw['Hora_en_parada'] -
                            df_forw['fecha_hora_evento']) / pd.Timedelta(seconds=1))

    df_forw.sort_values(by=['Patente', 'Hora_en_parada'], inplace=True)
    df_forw = df_forw.loc[~df_forw["evento_id_soc"].isna()]

    df_back = df_back[columnas_utiles_ele]
    df_forw = df_forw[columnas_utiles_ele]
    df_back.rename(columns=dict_col_ttec_back_ele, inplace=True)
    df_forw.rename(columns=dict_col_ttec_forw_ele, inplace=True)
    
    df_dia = df_back.merge(df_forw[col_forw_ele], left_index=True,
                             right_index=True, suffixes=('', ''))

    df_f.append(df_dia.copy())
    logger.info(f' . . . ')
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
        procesar_dia_ele(fecha_)

    logger.info('Listo todo para esta semana')
    os.chdir('..')
    return None


if __name__ == '__main__':
    global df
    global df_f
    mantener_log()
    # Crear variable que escribe en log file de este dia
    logger.info(f'Log file para este script de merge de data electrica en merged_data/merge_ele.log')
    file_handler = logging.FileHandler(f'merged_data/merge_ele.log')

    # no deja pasar los debug, solo info hasta critical
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(file_format)
    logger.addHandler(file_handler)

    direccion = 'data/adatrap_raw_data/data_descomprimida/'
    Servicios = os.listdir(direccion)
    logger.info(f'Servicios-sentido descargados: {Servicios}')
    for ss in Servicios:
        df_f = []
        logger.info(f'Leyendo data ADATRAP de {ss}')
        df = pd.read_csv(f'data/adatrap_raw_data/data_descomprimida/{ss}/Perfil.csv')

        logger.info(f'Datos ADATRAP total {ss}: {len(df.index)}')
        df = df.loc[df['Hora_en_parada'] != '0']
        df['Hora_en_parada'] = pd.to_datetime(df['Hora_en_parada'])
        df = df.loc[~df['Hora_en_parada'].isna()]
        df = df.loc[df['Expedición_inválida'] == 0]
        df.sort_values(by=['Hora_en_parada', 'Secuencia_parada'], inplace=True)
        df['Fecha'] = df['Hora_en_parada'].dt.date
        df = df[columnas_utiles_adatrap]
        logger.info(f'Datos ADATRAP con Hora_parada: {len(df.index)}')

        pipeline(2, 11, 2020, sem_especial=[2, 3, 4, 5, 6, 7])
        pipeline(9, 11, 2020, sem_especial=[1, 2, 3, 4, 6, 7])
        pipeline(16, 11, 2020, sem_especial=[1, 2, 3, 4, 5])
        logger.info('Listo todas las semanas')
        df_f = pd.concat(df_f)
        
        logger.info(f'Datos ADATRAP con Hora_parada: {len(df.index)}')
        logger.info(f'Data total del cruce: {len(df_f.index)}')
        if len(df_f.index) > 1000:
            logger.info('Guardando cruce')
            df_f['Fecha'] = pd.to_datetime(df_f['Fecha'])
            df_f.to_parquet(f'merged_data/Cruce_Adatrap_Ttec_ele_{ss}.parquet', compression='gzip')
        else:
            logger.warning('Muy pocos datos, no se va a guardar')
        logger.info(f'Listo todo para servicio {ss}')
        logger.info(' * * * . . . . . . . * * *  ')
