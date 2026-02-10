import pandas as pd
import os

def procesar_afiliaciones(nombre_hoja):
    # 1. Rutas
    ruta_script = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(ruta_script, "..", "Fuente", "Afiliación 2025.xlsx")
    file_path = os.path.normpath(file_path)

    print(f">>> Intentando leer: {file_path}")

    if not os.path.exists(file_path):
        print(f"!!! ERROR: No existe el archivo en {file_path}")
        return None

    # 2. Carga
    try:
        df_raw = pd.read_excel(file_path, sheet_name=nombre_hoja, header=None, dtype=str)
        print(f">>> Hoja {nombre_hoja} cargada correctamente. Filas: {len(df_raw)}")
    except Exception as e:
        print(f"!!! ERROR al leer la hoja: {e}")
        return None

    # 3. Lógica de bloques
    fila_fechas = 1
    fila_datos_inicio = 3
    lista_dataframes = []
    
    # Columnas fijas
    cols_fijas = df_raw.iloc[fila_datos_inicio:, 0:4].copy()
    cols_fijas.columns = ["Afiliacion", "Ope_Proy", "Solicitud_Nuevos", "Solicitud_Reconectados"]

    for i in range(4, df_raw.shape[1], 5):
        fecha = df_raw.iloc[fila_fechas, i]
        if pd.isna(fecha) or "cumplimiento" in str(fecha).lower():
            continue

        print(f"    -> Procesando semana: {fecha}")
        bloque = df_raw.iloc[fila_datos_inicio:, i:i+5].copy()
        bloque.columns = ["Bolsa", "Nuevos", "Total", "Congelas", "Activas"]
        
        df_temp = pd.concat([cols_fijas.reset_index(drop=True), bloque.reset_index(drop=True)], axis=1)
        df_temp["Fecha_Corte"] = fecha
        lista_dataframes.append(df_temp)

    if not lista_dataframes:
        print("!!! ADVERTENCIA: No se generó ningún dato.")
        return None

    # 4. Consolidación y Limpieza
    df_final = pd.concat(lista_dataframes, ignore_index=True)
    df_final = df_final.dropna(subset=["Afiliacion"])
    
    # Filtro robusto
    df_final = df_final[~df_final["Afiliacion"].astype(str).str.contains("Total|Afiliación|Plazas", case=False, na=False)]
    
    return df_raw

def ajuste_parametros(df):
    #fechas
    lista_fechas = [
        df.iloc[1, 4], 
        df.iloc[1, 9], 
        df.iloc[1, 14], 
        df.iloc[1, 19],
        df.iloc[1, 24]
    ]
    lista_fechas = pd.DataFrame(lista_fechas)
    lista_fechas.columns=['Fecha']
    lista_fechas.reset_index(drop=True, inplace=True)
    lista_fechas.dropna(inplace=True)
    lista_fechas.head()
    #unidades de negocio
    df_unidades_negocio = df.iloc[3:19, 1:2].copy()
    df_unidades_negocio.columns = ['Unidad_Negocio']


    df_unidades_negocio.dropna(inplace=True)


    df_unidades_negocio = df_unidades_negocio[
        ~df_unidades_negocio['Unidad_Negocio'].astype(str).str.contains("Total|flota|Plazas", case=False)
    ]

    df_unidades_negocio.reset_index(drop=True, inplace=True)

    df_unidades_negocio

    return df_unidades_negocio, lista_fechas

def consolidado_mes(lista_fechas, df_unidades_negocio, df):
    df_consolidado = []
    df_recorte = [] 
    posFecha = 4
    sem = 1
    for fecha in lista_fechas['Fecha']: 
        for unidad in df_unidades_negocio['Unidad_Negocio']:
            idx = df[df.iloc[:, 1] == unidad].index[0]

            fecha_str = str(fecha).split(" -")[0].strip()
            

            df_aux = [
                fecha_str,
                sem,
                df.iloc[idx, 1],         # Nombre Unidad
                df.iloc[idx, 2],         # Atributo 1 (C)
                df.iloc[idx, 3],         # Atributo 2 (D)
                df.iloc[idx, posFecha],     # Bolsa
                df.iloc[idx, posFecha + 1], # Nuevos
                df.iloc[idx, posFecha + 2], # Total
                df.iloc[idx, posFecha + 3], # Congelas
                df.iloc[idx, posFecha + 4]  # Activas
            ]
            df_consolidado.append(df_aux)
            df_aux = []
        sem = sem + 1
        posFecha = posFecha + 5
    df_consolidado = pd.DataFrame(df_consolidado)
    df_consolidado.columns = ['Fecha Semana', 'No Semana', 'Afiliación', 
    'Solicitud Nuevos',
    'Solicitud Reconectados', 'Activas', 'Bolsa', 'Nuevos', 'Total', 'Congelados']
    return df_consolidado
    

if __name__ == "__main__":
    print("=== INICIANDO PROCESO ===")
    columnas_procesar = ["MARZO", "ABRIL", "MAYO","JUNIO","JULIO","AGOSTO",
    "SEPTIEMBRE","OCTUBRE","NOVIEMBRE","DICIEMBRE","ENERO-26","FEBRERO-26"]
    consolidado = []
    for columna in columnas_procesar:
        df_resultado = procesar_afiliaciones(columna)
        if df_resultado is not None and not df_resultado.empty:
            print("Lectura Correcta de la hoja: ", columna)
            df_unidades_negocio, lista_fechas = ajuste_parametros(df_resultado)
            consolidado_aux = consolidado_mes(lista_fechas, df_unidades_negocio, df_resultado)
            consolidado.append(consolidado_aux)
            print(df_unidades_negocio)
            print(lista_fechas)
        else:
            print("!!! El proceso terminó pero el resultado está vacío.")
    consolidado = pd.concat(consolidado, ignore_index=True)
    consolidado.to_csv("../Fuente/consolidado.csv", index=False)