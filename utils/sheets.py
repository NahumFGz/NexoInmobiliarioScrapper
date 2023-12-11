import pygsheets
import pandas as pd


def autentificar(rutaCredenciales, nombreExcel, key='name'):
    gc = pygsheets.authorize(service_file=rutaCredenciales)

    if key == 'name':
        sh = gc.open(nombreExcel)
    elif key == 'id':
        sh = gc.open_by_key(nombreExcel)
    elif key == 'url':
        sh = gc.open_by_url(nombreExcel)

    return gc, sh

def read_sheet(sh, nombreHoja):
    worksheet = sh[1].worksheet_by_title(nombreHoja)
    dataframe = worksheet.get_as_df(numerize=False)
    
    return dataframe

def dataframe_to_sheet(sh, newDataFrame, nombreHojaDestino):
    worksheet = sh[1].worksheet_by_title(nombreHojaDestino)
    try:
        worksheet.rows = newDataFrame.shape[0] + 1
        worksheet.cols = newDataFrame.shape[1] + 1
    except:
        worksheet.rows = 2
        worksheet.cols = newDataFrame.shape[1]

    worksheet.set_dataframe(newDataFrame, start='A1', copy_index=False)

def clean_sheet(sh, nombreHojaDestino):
    # Obtener dimensiones de la hoja destino
    df_sheet = read_sheet(sh, nombreHojaDestino)
    num_rows, num_cols =  df_sheet.shape
    
    # Crear dataframe vacio
    data = {}
    for i in range(num_cols):
        # Creamos una lista vacía para cada columna
        data[f'column_{i+1}'] = [''] * num_rows

    df_vacio = pd.DataFrame(data)
    
    # Guardar dataframe vacio en hoja destino
    dataframe_to_sheet(sh, df_vacio,nombreHojaDestino)


def delete_rows_per_date(streamSheet, codigoExcel, nombreHoja, nombreColumna, parametroBorrar, operadorComparacion):
    from datetime import datetime
    
    # Asignar variables de autentificación
    gc,  sh = streamSheet[0], streamSheet[1]
    
    # Seleccionar hoja de trabajo
    wks = sh.worksheet_by_title(nombreHoja)

    # Eliminamos la ultima fecha
    values = wks.get_all_values(value_render="FORMATTED_VALUE")
    columna = list(values[0]).index(nombreColumna)
    
    deleteRows = []
    for i, r in enumerate(values):
        if '-' in str(r):
            if operadorComparacion(datetime.strptime(r[columna], "%Y-%m-%d"), datetime.strptime(parametroBorrar, "%Y-%m-%d")):
                deleteRows.append(i)
    
    if deleteRows != []:
        reqs = [
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": wks.id,
                        "startIndex": e,
                        "endIndex": e + 1,
                        "dimension": "ROWS",
                    }
                }
            }
            for e in deleteRows
        ]
        reqs.reverse()
        if len(reqs) > 0:
            gc.sheet.batch_update(codigoExcel, reqs)