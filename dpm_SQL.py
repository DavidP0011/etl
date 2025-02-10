# @title map_elements_dynamic_df()
import pandas as pd
from rapidfuzz import process, fuzz
from googletrans import Translator
import unicodedata
import re
def map_elements_dynamic_df(params: dict) -> pd.DataFrame:
    """
    Mapea una lista de valores (elements_list_raw_value) contra una lista de referencia
    (elements_list_reference_value) usando RapidFuzz y permite traducir previamente los valores
    originales al inglés con Google Translate API si se activa la opción.

    Argumentos:
    -----------
    params: dict con las siguientes claves:
        - "elements_list_raw_value": list
        - "elements_list_reference_value": list
        - "threshold_similarity": float (entre 0 y 1)
        - "translate_to_english": bool (opcional, traducir elementos originales al inglés)
        - "elements_list_raw_context_prefix": str (prefijo para añadir contexto a los valores originales antes de traducir)

    Retorna:
    --------
    DataFrame con columnas:
        - "original_value": valor crudo
        - "cleaned_value": valor limpio sin tildes ni caracteres especiales
        - "translated_value": valor traducido al inglés (con prefijo si se especifica)
        - "translated_value_cleaned": valor traducido al inglés sin prefijo y en minúsculas
        - "mapped_value": mejor coincidencia en la lista de referencia
        - "similarity_score": valor de la similitud en escala 0-100
    """

    def clean_text(text):
        # Eliminar tildes y caracteres especiales
        text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8')
        text = re.sub(r'[^a-zA-Z0-9\s]', '', text)  # Quitar caracteres especiales
        return text.strip()

    def remove_country_prefix(text):
        # Eliminar el prefijo "The country called" (insensible a mayúsculas/minúsculas) en cualquier posición
        return re.sub(r'\bthe country called\b', '', text, flags=re.IGNORECASE).strip().lower()

    # Extraemos los parámetros del diccionario
    elements_list_raw_value = params.get("elements_list_raw_value", [])
    elements_list_reference_value = params.get("elements_list_reference_value", [])
    threshold_similarity = params.get("threshold_similarity", 0.5)  # valor por defecto
    translate_to_english = params.get("translate_to_english", False)
    context_prefix = params.get("elements_list_raw_context_prefix", "")

    resultados = []
    cleaned_values = [clean_text(value) for value in elements_list_raw_value]

    if translate_to_english:
        translator = Translator()
        elements_list_translated = []
        elements_list_translated_cleaned = []
        print("\n--- Inicio de la traducción ---")
        for element in cleaned_values:
            try:
                # Añadir el prefijo en español antes de traducir
                element_with_context = f"{context_prefix} {element}".strip()
                print(f"Traduciendo: '{element_with_context}'...")
                # Especificamos que el texto original está en español (src="es")
                translated = translator.translate(element_with_context, src="es", dest="en").text
                # Verificar si la traducción es incorrecta (por ejemplo, "Country Country")
                if "country" in translated.lower() and translated.lower().count("country") > 1:
                    print(f"Error: Traducción incorrecta detectada: '{translated}'. Usando valor original.")
                    translated = element  # Usar el valor original si la traducción es incorrecta
                # Guardar el valor traducido con prefijo
                elements_list_translated.append(translated)
                # Eliminar el prefijo "country" y convertir a minúsculas
                translated_cleaned = remove_country_prefix(translated)
                elements_list_translated_cleaned.append(translated_cleaned)
                print(f"Resultado traducido: '{translated}' (sin prefijo y en minúsculas: '{translated_cleaned}')")
            except Exception as e:
                # En caso de error de traducción, mantener el valor original
                elements_list_translated.append(element)
                elements_list_translated_cleaned.append(element.lower())  # Convertir a minúsculas
                print(f"Error al traducir '{element}': {e}")
        print("--- Fin de la traducción ---\n")
    else:
        elements_list_translated = cleaned_values
        elements_list_translated_cleaned = [value.lower() for value in cleaned_values]  # Convertir a minúsculas

    print("\n--- Inicio del mapeo ---")
    for original, cleaned, translated, translated_cleaned in zip(
        elements_list_raw_value, cleaned_values, elements_list_translated, elements_list_translated_cleaned
    ):
        # Extraemos la mejor coincidencia usando el valor traducido sin prefijo y en minúsculas
        print(f"Mapeando: Original: '{original}', Limpio: '{cleaned}', Traducido: '{translated}'...")
        best_match = process.extractOne(
            translated_cleaned,  # Usamos el valor traducido sin prefijo y en minúsculas
            elements_list_reference_value,
            scorer=fuzz.ratio  # se puede cambiar por fuzz.partial_ratio u otros
        )

        if best_match:
            match_value, match_score, _ = best_match
            print(f"Mejor coincidencia: '{match_value}' con una similitud de {match_score}")
            resultados.append({
                "original_value": original,
                "cleaned_value": cleaned,
                "translated_value": translated if translate_to_english else None,
                "translated_value_cleaned": translated_cleaned if translate_to_english else None,
                "mapped_value": match_value,
                "similarity_score": match_score
            })
        else:
            # Sin coincidencias (caso muy raro si la lista de referencia no está vacía)
            print("No se encontró coincidencia")
            resultados.append({
                "original_value": original,
                "cleaned_value": cleaned,
                "translated_value": translated if translate_to_english else None,
                "translated_value_cleaned": translated_cleaned if translate_to_english else None,
                "mapped_value": None,
                "similarity_score": 0
            })
    print("--- Fin del mapeo ---\n")

    df_resultado = pd.DataFrame(resultados)

    # Logging de estadísticas
    total = len(elements_list_raw_value)
    print("\n--- Estadísticas del proceso ---")
    print(f"Total de elementos procesados: {total}")
    print("----------------------------------\n")

    return df_resultado

# @title GBQ_tables_fields_info_df
def GBQ_tables_fields_info_df(config):
    """
    Retorna un DataFrame con la información de datasets, tablas y campos de un proyecto de BigQuery.

    Args:
        config (dict): Diccionario de configuración con:
            - 'project_id' (str) [requerido]: El ID del proyecto de BigQuery.
            - 'datasets' (list) [opcional]: Lista de los IDs de los datasets a consultar.
              Si no se proporciona, se consultan todos los disponibles en el proyecto.
            - 'include_tables' (bool) [opcional]: Indica si se deben incluir las tablas
              en el esquema. Por defecto es True.

    Returns:
        pd.DataFrame: DataFrame con las columnas:
            [
                'project_id',
                'dataset_id',
                'table_name',
                'field_name',
                'field_type',
                'num_rows',
                'num_columns',
                'size_mb'
            ]
    """
    from google.cloud import bigquery
    from google.colab import auth
    import pandas as pd

    # 1. Autenticamos (en caso de estar en Colab)
    auth.authenticate_user()

    project_id = config.get('project_id')
    if not project_id:
        raise ValueError("El 'project_id' es un argumento requerido en el diccionario de configuración.")

    # Parámetros adicionales
    datasets_incluidos = config.get('datasets', None)
    include_tables = config.get('include_tables', True)

    # 2. Creamos el cliente de BigQuery
    client = bigquery.Client(project=project_id)

    # 3. Obtenemos la lista de datasets
    if datasets_incluidos:
        datasets = [client.get_dataset(f"{project_id}.{dataset_id}") for dataset_id in datasets_incluidos]
    else:
        datasets = list(client.list_datasets(project=project_id))

    # Estructura temporal para ir almacenando la info de tablas y campos
    tables_info_list = []

    for dataset in datasets:
        dataset_id = dataset.dataset_id
        full_dataset_id = f"{project_id}.{dataset_id}"

        # 4. Si include_tables=True, listamos las tablas de cada dataset
        if include_tables:
            tables = list(client.list_tables(full_dataset_id))
            for table_item in tables:
                table_ref = client.get_table(table_item.reference)  # Obtenemos objeto Table

                # Datos de la tabla
                table_name = table_item.table_id
                num_rows = table_ref.num_rows
                num_columns = len(table_ref.schema)
                size_mb = table_ref.num_bytes / (1024 * 1024)  # Convertir de bytes a MB

                # 5. Campos (schema) de la tabla
                fields = table_ref.schema

                # Cada campo de la tabla se agrega como un registro independiente
                if fields:
                    for field in fields:
                        tables_info_list.append({
                            'project_id': project_id,
                            'dataset_id': dataset_id,
                            'table_name': table_name,
                            'field_name': field.name,
                            'field_type': field.field_type,
                            'num_rows': num_rows,
                            'num_columns': num_columns,
                            'size_mb': round(size_mb, 2)
                        })
                else:
                    # Si la tabla no tiene campos, se ingresa un registro con field_name=None
                    tables_info_list.append({
                        'project_id': project_id,
                        'dataset_id': dataset_id,
                        'table_name': table_name,
                        'field_name': None,
                        'field_type': None,
                        'num_rows': num_rows,
                        'num_columns': num_columns,
                        'size_mb': round(size_mb, 2)
                    })
        else:
            # En caso de no incluir tablas, añadimos el dataset sin info de tablas
            # (aunque rara vez se desea esto en un DataFrame).
            tables_info_list.append({
                'project_id': project_id,
                'dataset_id': dataset_id,
                'table_name': None,
                'field_name': None,
                'field_type': None,
                'num_rows': None,
                'num_columns': None,
                'size_mb': None
            })

    # 6. Convertimos la lista de diccionarios en DataFrame
    df_tables_fields = pd.DataFrame(tables_info_list)

    return df_tables_fields



# @title fields_name_format()
def fields_name_format(config):
    """
    Formatea nombres de campos de datos según configuraciones específicas.

    Args:
        config (dict): Diccionario de configuración con los siguientes parámetros:
            - fields_name_raw_list (list): Lista de nombres de campos a formatear.
            - formato_final (str, opcional): Formato final deseado para los nombres
              ('CamelCase', 'snake_case', 'Sentence case', None o False para no formatear).
            - reemplazos (dict, opcional): Diccionario de términos a reemplazar.
            - siglas (list, opcional): Lista de siglas que deben mantenerse en mayúsculas.

    Returns:
        pandas.DataFrame: DataFrame con los campos originales y formateados.
    """
    import re
    import pandas as pd
    
    fields_name_raw_list = config.get('fields_name_raw_list', [])
    formato_final = config.get('formato_final', 'CamelCase')
    reemplazos = config.get('reemplazos', {})
    siglas = [sigla.upper() for sigla in config.get('siglas', [])]

    if not isinstance(fields_name_raw_list, list) or not fields_name_raw_list:
        raise ValueError("El parámetro 'fields_name_raw_list' debe ser una lista no vacía.")

    def aplicar_reemplazos(field, reemplazos):
        """Aplica reemplazos definidos en el diccionario."""
        for key, value in sorted(reemplazos.items(), key=lambda x: -len(x[0])):  # Orden por longitud descendente
            if key in field:
                field = field.replace(key, value)
        return field

    def formatear_campo(field, formato, siglas):
        """Formatea el campo según el formato especificado.
           Si formato es None o False, no se hace formateo alguno."""
        if formato is None or formato is False:
            # Sin formateo, se devuelve tal cual.
            return field

        words = [w for w in re.split(r'[_\-\s]+', field) if w]

        if formato == 'CamelCase':
            return ''.join(
                word.upper() if word.upper() in siglas
                else word.capitalize() if idx == 0
                else word.lower()
                for idx, word in enumerate(words)
            )
        elif formato == 'snake_case':
            return '_'.join(
                word.upper() if word.upper() in siglas
                else word.lower() for word in words
            )
        elif formato == 'Sentence case':
            return ' '.join(
                word.upper() if word.upper() in siglas
                else word.capitalize() if idx == 0
                else word.lower()
                for idx, word in enumerate(words)
            )
        else:
            raise ValueError(f"Formato '{formato}' no soportado.")

    resultado = []
    for field in fields_name_raw_list:
        original_field = field
        # Procesar campos
        field = aplicar_reemplazos(field, reemplazos)
        formatted_field = formatear_campo(field, formato_final, siglas)

        resultado.append({'Campo Original': original_field, 'Campo Formateado': formatted_field})

    return pd.DataFrame(resultado)

# @title SQL_generate_cleaning_str()
def SQL_generate_cleaning_str(params: dict) -> str:
    """
    Genera una sentencia SQL para crear o sobrescribir una tabla de 'staging'
    aplicando mapeos de columnas, filtros y prefijo opcional a los campos destino.

    Args:
        params (dict):
            - table_source (str): Nombre de la tabla fuente (proyecto.dataset.tabla).
            - table_destination (str): Nombre de la tabla destino (proyecto.dataset.tabla).
            - fields_mapped_use (bool): Si True, usa la columna 'Campo Formateado' como nombre en el SELECT.
            - fields_mapped_df (pd.DataFrame): DataFrame con columnas ["Campo Original", "Campo Formateado"].
            - fields_destination_prefix (str, opcional): Prefijo a aplicar a los campos mapeados.
            - exclude_records_by_creation_date_bool (bool): Si True, filtra por rango de fechas.
            - exclude_records_by_creation_date_field (str, opcional): Campo con fecha de creación.
            - exclude_records_by_creation_date_range (dict, opcional): { "from": "YYYY-MM-DD", "to": "YYYY-MM-DD" }.
            - exclude_records_by_keywords_bool (bool): Si True, aplica exclusión usando palabras clave.
            - exclude_records_by_keywords_fields (list, opcional): Campos a los que se aplicará exclusión.
            - exclude_records_by_keywords_words (list, opcional): Lista de términos a excluir.
            - fields_to_trim (list, opcional): Columnas a las que se aplicará TRIM.

    Returns:
        str: Sentencia SQL generada.

    Raises:
        ValueError: Si faltan parámetros requeridos o son inválidos.
    """
    import pandas as pd

    # Validación de parámetros obligatorios
    table_source = params.get("table_source")
    table_destination = params.get("table_destination")
    fields_mapped_df = params.get("fields_mapped_df")
    fields_mapped_use = params.get("fields_mapped_use", True)
    fields_destination_prefix = params.get("fields_destination_prefix", "")

    if not table_source or not table_destination:
        raise ValueError("Los parámetros 'table_source' y 'table_destination' son obligatorios.")
    if not isinstance(fields_mapped_df, pd.DataFrame):
        raise ValueError("'fields_mapped_df' debe ser un DataFrame válido.")

    # Parámetros opcionales con valores predeterminados
    exclude_records_by_creation_date_bool = params.get("exclude_records_by_creation_date_bool", False)
    exclude_records_by_creation_date_field = params.get("exclude_records_by_creation_date_field", "")
    exclude_records_by_creation_date_range = params.get("exclude_records_by_creation_date_range", {})

    exclude_records_by_keywords_bool = params.get("exclude_records_by_keywords_bool", False)
    exclude_records_by_keywords_fields = params.get("exclude_records_by_keywords_fields", [])
    exclude_records_by_keywords_words = params.get("exclude_records_by_keywords_words", [])
    fields_to_trim = params.get("fields_to_trim", [])

    # Generar la lista de campos para el SELECT con prefijo aplicado
    select_clauses = []
    for _, row in fields_mapped_df.iterrows():
        campo_origen = row['Campo Original']
        
        # Si fields_mapped_use es True, usar el nombre formateado, si no, el original
        campo_destino = f"{fields_destination_prefix}{row['Campo Formateado']}" if fields_mapped_use else f"{fields_destination_prefix}{campo_origen}"

        if campo_origen in fields_to_trim:
            # Si el campo está en fields_to_trim, aplica TRIM y REPLACE
            select_clause = f"TRIM(REPLACE(`{campo_origen}`, '  ', ' ')) AS `{campo_destino}`"
        else:
            # Campo formateado con prefijo
            select_clause = f"`{campo_origen}` AS `{campo_destino}`"

        select_clauses.append(select_clause)

    select_part = ",\n  ".join(select_clauses)

    # Construir la cláusula WHERE en base a filtros
    where_filters = []

    # Filtro de rango de fechas
    if exclude_records_by_creation_date_bool and exclude_records_by_creation_date_field:
        date_from = exclude_records_by_creation_date_range.get("from", "")
        date_to = exclude_records_by_creation_date_range.get("to", "")
        if date_from:
            where_filters.append(f"`{exclude_records_by_creation_date_field}` >= '{date_from}'")
        if date_to:
            where_filters.append(f"`{exclude_records_by_creation_date_field}` <= '{date_to}'")

    # Filtro de exclusión de términos
    if exclude_records_by_keywords_bool and exclude_records_by_keywords_fields and exclude_records_by_keywords_words:
        for field in exclude_records_by_keywords_fields:
            where_filters.extend([f"`{field}` NOT LIKE '%{word}%'" for word in exclude_records_by_keywords_words])

    # Construcción de la cláusula WHERE
    where_clause = " AND ".join(where_filters) if where_filters else "TRUE"

    # Construcción final del SQL
    SQL_script = f"""
CREATE OR REPLACE TABLE `{table_destination}` AS
SELECT
  {select_part}
FROM `{table_source}`
WHERE {where_clause}
;
""".strip()

    return SQL_script



# @title SQL_generate_academic_date_str()

def SQL_generate_academic_date_str(params):
    """
    Crea o reemplaza una tabla donde se agregan campos de fecha "académica" o "fiscal"
    basándose en reglas de corte (start_month, start_day) sobre un campo fecha existente.

    Parámetros esperados (dict):
      - table_source (str): Tabla de origen (puede ser la misma 'staging' previa).
      - table_destination (str): Tabla destino que se creará o reemplazará.
      - custom_fields_config (dict): Estructura:
          {
            "nombre_de_campo_fecha": [
                {"start_month": 9, "start_day": 1, "suffix": "fiscal"},
                {"start_month": 10, "start_day": 1, "suffix": "academico"}
            ]
          }

    Retorna:
      (str): Sentencia SQL para crear o reemplazar la tabla con las columnas adicionales.
    """
    table_source = params["table_source"]
    table_destination = params["table_destination"]
    custom_fields_config = params["custom_fields_config"]

    # Vamos a generar un SELECT con todas las columnas de la tabla_source más las columnas nuevas
    additional_expressions = []

    for field, configs in custom_fields_config.items():
        for cfg in configs:
            start_month = cfg.get("start_month", 9)
            start_day = cfg.get("start_day", 1)
            suffix = cfg.get("suffix", "custom")

            new_field = f"{field}_{suffix}"

            expression = f"""
CASE
  WHEN (EXTRACT(MONTH FROM `{field}`) > {start_month})
       OR (EXTRACT(MONTH FROM `{field}`) = {start_month} AND EXTRACT(DAY FROM `{field}`) >= {start_day}) THEN
    CONCAT(
      LPAD(CAST((EXTRACT(YEAR FROM `{field}`) - 2000) AS STRING), 2, '0'),
      '-',
      LPAD(CAST((EXTRACT(YEAR FROM `{field}`) + 1 - 2000) AS STRING), 2, '0')
    )
  ELSE
    CONCAT(
      LPAD(CAST((EXTRACT(YEAR FROM `{field}`) - 1 - 2000) AS STRING), 2, '0'),
      '-',
      LPAD(CAST((EXTRACT(YEAR FROM `{field}`) - 2000) AS STRING), 2, '0')
    )
END AS `{new_field}`
""".strip()
            additional_expressions.append(expression)

    # Unir las expresiones adicionales
    additional_select = ",\n  ".join(additional_expressions) if additional_expressions else ""

    # Construir el script SQL
    if additional_select:
        SQL_script = f"""
CREATE OR REPLACE TABLE `{table_destination}` AS
SELECT
  s.*,
  {additional_select}
FROM `{table_source}` s
;
""".strip()
    else:
        # Si no hay configuraciones, simplemente se clona la tabla
        SQL_script = f"""
CREATE OR REPLACE TABLE `{table_destination}` AS
SELECT
  *
FROM `{table_source}`;
""".strip()

    return SQL_script

# @title SQL_generate_join_tables_str()
def SQL_generate_join_tables_str(params: dict) -> str:
    """
    Crea (o reemplaza) una tabla uniendo:
    - Tabla primaria (table_source_primary)
    - Tabla secundaria (table_source_secondary)
    - (Opcional) Tabla puente (table_source_bridge)

    Obtiene las columnas reales de cada tabla usando INFORMATION_SCHEMA, y les
    aplica un prefijo para evitar duplicados en el SELECT.

    Args:
        params (dict):
            - table_source_primary (str): Nombre de la tabla primaria (proyecto.dataset.tabla).
            - table_source_primary_id_field (str): Campo de unión en la tabla primaria.
            - table_source_secondary (str): Nombre de la tabla secundaria (proyecto.dataset.tabla).
            - table_source_secondary_id (str): Campo de unión en la tabla secundaria.
            - table_source_bridge_use (bool): Indica si se empleará la tabla puente.
            - table_source_bridge (str, opcional): Tabla puente, si `table_source_bridge_use`=True (proyecto.dataset.tabla).
            - table_source_bridge_ids_fields (dict, opcional): Diccionario con
              { 'primary_id': '...', 'secondary_id': '...' } para la tabla puente.
            - join_type (str, opcional): Tipo de JOIN (INNER, LEFT, RIGHT, FULL, CROSS). Por defecto "LEFT".
            - join_field_prefixes (dict, opcional): Prefijos para primary, secondary, bridge.
              Por defecto {"primary": "p_", "secondary": "s_", "bridge": "b_"}.
            - table_destination (str): Tabla destino (proyecto.dataset.tabla).

    Returns:
        str: Sentencia SQL para crear o reemplazar la tabla destino.

    Raises:
        ValueError: Si los parámetros obligatorios están ausentes o son inválidos.
    """
    import re
    from google.cloud import bigquery

    # Extraer parámetros básicos
    table_source_primary = params["table_source_primary"]
    table_source_primary_id_field = params["table_source_primary_id_field"]
    table_source_secondary = params["table_source_secondary"]
    table_source_secondary_id = params["table_source_secondary_id"]

    table_source_bridge_use = params.get("table_source_bridge_use", False)
    table_source_bridge = params.get("table_source_bridge", "")
    table_source_bridge_ids_fields = params.get("table_source_bridge_ids_fields", {})

    join_type = params.get("join_type", "LEFT").upper()
    valid_join_types = ["INNER", "LEFT", "RIGHT", "FULL", "CROSS"]
    if join_type not in valid_join_types:
        raise ValueError(f"El join_type '{join_type}' no es válido. Debe ser uno de {valid_join_types}.")

    join_field_prefixes = params.get("join_field_prefixes", {"primary": "p_", "secondary": "s_", "bridge": "b_"})
    table_destination = params["table_destination"]

    # Helper para extraer dataset y table_id
    def split_dataset_table(full_name: str):
        """
        Espera un formato "dataset.table" o "project.dataset.table".
        Retorna (project, dataset, table) para usar en queries.
        """
        parts = full_name.split(".")
        if len(parts) == 2:
            return (project_id, parts[0], parts[1])  # Asumimos dataset.table
        elif len(parts) == 3:
            return (parts[0], parts[1], parts[2])  # Asumimos project.dataset.table
        else:
            raise ValueError(f"Nombre de tabla inválido: {full_name}")

    # Función para obtener columnas reales de una tabla
    def get_table_columns(full_table_name: str):
        """
        Retorna la lista de columnas de `full_table_name` consultando INFORMATION_SCHEMA.
        """
        proj, dset, tbl = split_dataset_table(full_table_name)
        client = bigquery.Client(project=proj)

        query = f"""
        SELECT column_name
        FROM `{proj}.{dset}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{tbl}'
        ORDER BY ordinal_position
        """
        rows = client.query(query).result()
        col_list = [row.column_name for row in rows]
        return col_list

    # Obtener columnas de cada tabla
    primary_cols = get_table_columns(table_source_primary)
    secondary_cols = get_table_columns(table_source_secondary)
    bridge_cols = []
    if table_source_bridge_use and table_source_bridge:
        bridge_cols = get_table_columns(table_source_bridge)

    # Construir SELECT renombrando las columnas con prefijos
    primary_selects = [
        f"{join_field_prefixes['primary']}.{col} AS {join_field_prefixes['primary']}{col}"
        for col in primary_cols
    ]
    secondary_selects = [
        f"{join_field_prefixes['secondary']}.{col} AS {join_field_prefixes['secondary']}{col}"
        for col in secondary_cols
    ]
    bridge_selects = []
    if table_source_bridge_use and bridge_cols:
        bridge_selects = [
            f"{join_field_prefixes['bridge']}.{col} AS {join_field_prefixes['bridge']}{col}"
            for col in bridge_cols
        ]

    all_selects = primary_selects + secondary_selects + bridge_selects
    select_clause = ",\n  ".join(all_selects)

    # Construir la cláusula FROM + JOIN
    if table_source_bridge_use and table_source_bridge:
        join_clause = f"""
FROM `{table_source_primary}` AS {join_field_prefixes['primary']}
{join_type} JOIN `{table_source_bridge}` AS {join_field_prefixes['bridge']}
  ON {join_field_prefixes['bridge']}.{table_source_bridge_ids_fields['primary_id']} = {join_field_prefixes['primary']}.{table_source_primary_id_field}
{join_type} JOIN `{table_source_secondary}` AS {join_field_prefixes['secondary']}
  ON {join_field_prefixes['bridge']}.{table_source_bridge_ids_fields['secondary_id']} = {join_field_prefixes['secondary']}.{table_source_secondary_id}
"""
    else:
        join_clause = f"""
FROM `{table_source_primary}` AS {join_field_prefixes['primary']}
{join_type} JOIN `{table_source_secondary}` AS {join_field_prefixes['secondary']}
  ON {join_field_prefixes['primary']}.{table_source_primary_id_field} = {join_field_prefixes['secondary']}.{table_source_secondary_id}
"""

    # Armar la sentencia final
    SQL_script = f"""
CREATE OR REPLACE TABLE `{table_destination}` AS
SELECT
  {select_clause}
{join_clause}
;
""".strip()

    return SQL_script

# @title SQL_generate_CPL_to_contacts_str()
def SQL_generate_CPL_to_contacts_str(params: dict) -> str:
    """
    Genera una sentencia SQL para crear o reemplazar una tabla que combina:
      1) Una tabla principal de contactos (table_source).
      2) Una tabla agregada con métricas por fecha y fuente (table_aggregated).
      3) Tablas dinámicas de métricas publicitarias.

    Args:
        params (dict): Diccionario con los parámetros:
            - table_destination (str): Nombre de la tabla resultado.
            - table_source (str): Tabla con información principal de contactos.
            - table_aggregated (str): Tabla agregada con métricas por fecha y fuente.
            - join_field (str): Campo de fecha para unión con aggregated.
            - join_on_source (str): Campo de fuente para unión con aggregated.
            - contact_creation_number (str): Campo que indica cuántos contactos se crearon.
            - ad_platforms (list): Lista de dicts con configuraciones de plataformas:
                - prefix (str): Prefijo identificador de la plataforma.
                - table (str): Nombre de la tabla de métricas publicitarias.
                - date_field (str): Campo de fecha para la unión.
                - source_value (str): Valor de fuente en la tabla de contactos.
                - total_* (str): Campos con métricas agregadas (e.g., field_total_spend).

    Returns:
        str: Sentencia SQL para crear o reemplazar la tabla combinada.

    Raises:
        ValueError: Si algún parámetro requerido falta o está vacío.
    """
    # Validar parámetros requeridos
    required_keys = [
        "table_destination", "table_source", "table_aggregated",
        "join_field", "join_on_source",
        "contact_creation_number", "ad_platforms"
    ]
    for key in required_keys:
        if key not in params or not params[key]:
            raise ValueError(f"El parámetro '{key}' es obligatorio y no puede estar vacío.")

    # Asignar variables a los parámetros
    table_destination = params["table_destination"]
    table_source = params["table_source"]
    table_aggregated = params["table_aggregated"]
    join_field = params["join_field"]
    join_on_source = params["join_on_source"]
    contact_creation_number = params["contact_creation_number"]
    ad_platforms = params["ad_platforms"]

    # Generar cláusulas SQL
    from_clause = f"""
FROM `{table_source}` o
LEFT JOIN `{table_aggregated}` a
  ON DATE(o.{join_field}) = a.{join_field}
  AND o.{join_on_source} = a.{join_on_source}
"""

    joins = []
    select_platform_metrics = []
    for idx, plat in enumerate(ad_platforms, start=1):
        alias = f"p{idx}"
        prefix = plat["prefix"]
        table = plat["table"]
        date_field = plat["date_field"]
        source_value = plat["source_value"]

        joins.append(f"""
LEFT JOIN `{table}` {alias}
  ON a.{join_field} = {alias}.{date_field}
""")

        for key, value in plat.items():
            if key.startswith("total_"):
                metric = key.replace("total_", "")
                col_name = f"contact_Ads_{prefix}_{metric}_by_day"
                expr = f"""
CASE
  WHEN a.{join_on_source} = "{source_value}" AND a.{contact_creation_number} > 0
    THEN {alias}.{value} / a.{contact_creation_number}
  ELSE NULL
END AS {col_name}
""".strip()
                select_platform_metrics.append(expr)

    final_select = ",\n".join(["o.*"] + select_platform_metrics)
    join_clause = "".join(joins)

    SQL_script = f"""
CREATE OR REPLACE TABLE `{table_destination}` AS
SELECT
  {final_select}
{from_clause}
{join_clause}
;
""".strip()

    return SQL_script

# @title SQL_generate_deal_ordinal_str()

def SQL_generate_deal_ordinal_str(params):
    """
    Crea o reemplaza una tabla que contiene todos los campos de la tabla fuente
    más un campo ordinal que indica el número de negocio por cada contacto (ordenado
    por la fecha de creación), filtrando por ciertos valores de un campo (p. ej. pipeline).

    Parámetros esperados (dict):
      - table_source (str): Tabla origen.
      - table_destination (str): Tabla destino.
      - contact_id_field (str): Campo que identifica al contacto.
      - deal_id_field (str): Campo que identifica al negocio.
      - deal_createdate_field (str): Campo de fecha de creación del negocio.
      - deal_filter_field (str): Campo a filtrar (p. ej. 'pipeline').
      - deal_filter_values (list): Lista de valores admitidos para el filter_field.
      - ordinal_field_name (str): Nombre de la columna ordinal resultante.

    Retorna:
      (str): Sentencia SQL para crear o reemplazar la tabla con el campo ordinal.
    """
    table_source = params["table_source"]
    table_destination = params["table_destination"]
    contact_id_field = params["contact_id_field"]
    deal_id_field = params["deal_id_field"]
    deal_createdate_field = params["deal_createdate_field"]
    deal_filter_field = params["deal_filter_field"]
    deal_filter_values = params["deal_filter_values"]
    deal_ordinal_field_name = params["deal_ordinal_field_name"]

    # Convertir la lista de valores del filtro en una cadena 'IN(...)'
    filter_str_list = ", ".join([f"'{v}'" for v in deal_filter_values])

    # Armar la subconsulta deals_filtered que calcula row_number
    # para los registros que cumplan el filtro
    SQL_script = f"""
CREATE OR REPLACE TABLE `{table_destination}` AS
WITH deals_filtered AS (
  SELECT
    {contact_id_field},
    {deal_id_field},
    ROW_NUMBER() OVER (
      PARTITION BY {contact_id_field}
      ORDER BY {deal_createdate_field}
    ) AS {deal_ordinal_field_name}
  FROM `{table_source}`
  WHERE {deal_filter_field} IN ({filter_str_list})
)
SELECT
  src.*,
  f.{deal_ordinal_field_name}
FROM `{table_source}` src
LEFT JOIN deals_filtered f
  ON src.{contact_id_field} = f.{contact_id_field}
  AND src.{deal_id_field} = f.{deal_id_field}
;
""".strip()

    return SQL_script

# @title SQL_generate_BI_view_str()
def SQL_generate_BI_view_str(params: dict) -> str:
    """
    Crea o reemplaza una vista (o tabla) tipo BI, seleccionando columnas
    de una tabla fuente con posibles mapeos de nombres, filtrado por rango
    de fechas y exclusión de registros marcados como borrados.

    Args:
        params (dict):
            - table_source (str): Tabla origen.
            - table_destination (str): Nombre de la vista o tabla destino.
            - fields_mapped_df (pd.DataFrame): DataFrame con ["Campo Original", "Campo Formateado"].
            - use_mapped_names (bool): Si True, usa nombres formateados.
            - creation_date_field (str, opcional): Campo de fecha para filtrar.
            - use_date_range (bool): Si True, filtra por rango de fechas.
            - date_range (dict, opcional): { "from": "YYYY-MM-DD", "to": "YYYY-MM-DD" }
            - exclude_deleted_records_bool (bool): Si True, excluye registros donde
              'exclude_deleted_records_field_name' = 'exclude_deleted_records_field_value'.
            - exclude_deleted_records_field_name (str, opcional): Nombre del campo que indica
              si el registro está “borrado”.
            - exclude_deleted_records_field_value (str, int, bool, etc., opcional): Valor que
              indica un registro “borrado”.

    Returns:
        str: Sentencia SQL para crear o reemplazar la vista/tabla BI.

    Raises:
        ValueError: Si faltan parámetros obligatorios como 'table_source', 'table_destination'
                    o 'fields_mapped_df'.
    """
    import pandas as pd

    # Validación de parámetros obligatorios
    table_source = params.get("table_source")
    table_destination = params.get("table_destination")
    fields_mapped_df = params.get("fields_mapped_df")

    if not table_source or not table_destination or not isinstance(fields_mapped_df, pd.DataFrame):
        raise ValueError("Faltan parámetros obligatorios: 'table_source', 'table_destination' o 'fields_mapped_df'.")

    use_mapped_names = params.get("use_mapped_names", True)
    creation_date_field = params.get("creation_date_field", "")
    date_range = params.get("date_range", {})
    use_date_range = params.get("use_date_range", False)
    exclude_deleted_records_bool = params.get("exclude_deleted_records_bool", False)
    exclude_deleted_records_field_name = params.get("exclude_deleted_records_field_name", "")
    exclude_deleted_records_field_value = params.get("exclude_deleted_records_field_value", None)

    # Construir SELECT
    select_cols = []
    for _, row in fields_mapped_df.iterrows():
        orig = row["Campo Original"]
        mapped = row["Campo Formateado"]
        if use_mapped_names:
            select_cols.append(f"`{orig}` AS `{mapped}`")
        else:
            select_cols.append(f"`{orig}`")

    select_clause = ",\n  ".join(select_cols)

    # Construir WHERE
    where_filters = []

    # Rango de fechas
    if use_date_range and creation_date_field:
        date_from = date_range.get("from", "")
        date_to = date_range.get("to", "")
        if date_from and date_to:
            where_filters.append(f"`{creation_date_field}` BETWEEN '{date_from}' AND '{date_to}'")
        elif date_from:
            where_filters.append(f"`{creation_date_field}` >= '{date_from}'")
        elif date_to:
            where_filters.append(f"`{creation_date_field}` <= '{date_to}'")

    # Excluir registros borrados (si se ha definido el campo y un valor que indique "borrado")
    if exclude_deleted_records_bool and exclude_deleted_records_field_name and exclude_deleted_records_field_value is not None:
        # Se excluyen registros donde el campo sea igual al valor "borrado"
        # => solo se incluyen los que NO cumplan esa condición
        # y los que sean NULL (si queremos conservarlos).
        where_filters.append(
            f"(`{exclude_deleted_records_field_name}` IS NULL OR `{exclude_deleted_records_field_name}` != {exclude_deleted_records_field_value})"
        )

    where_clause = " AND ".join(where_filters) if where_filters else "TRUE"

    # Generar la sentencia final (vista o tabla, se asume vista)
    sql_script_str = f"""
CREATE OR REPLACE VIEW `{table_destination}` AS
SELECT
  {select_clause}
FROM `{table_source}`
WHERE {where_clause}
;
""".strip()

    return sql_script_str
    
    
    # @title GBQ_execute_SQL()
def GBQ_execute_SQL(params: dict) -> None:
    """
    Ejecuta un script SQL en Google BigQuery y muestra un resumen detallado con estadísticas del proceso, incluyendo logs de la API y el conteo de filas en la tabla de destino.
    
    Args:
        params (dict):
            - GCP_project_id (str): ID del proyecto de GCP.
            - SQL_script (str): Script SQL a ejecutar.
    
    Raises:
        ValueError: Si faltan los parámetros necesarios.
        google.api_core.exceptions.GoogleAPIError: Si ocurre un error durante la ejecución del script.
    """
    from google.cloud import bigquery
    from google.api_core.exceptions import GoogleAPIError
    import time

    # Validación de parámetros
    GCP_project_id = params.get('GCP_project_id')
    SQL_script = params.get('SQL_script')
    
    if not GCP_project_id or not SQL_script:
        raise ValueError("Faltan los parámetros 'GCP_project_id' o 'SQL_script'.")

    print("[INFO]: Iniciando el cliente de BigQuery...")
    start_time = time.time()
    
    # Inicializar el cliente de BigQuery
    GBQclient = bigquery.Client(project=GCP_project_id)
    print(f"[INFO]: Cliente de BigQuery inicializado en el proyecto: {GCP_project_id}\n")
    
    try:
        # Extraer información del script SQL
        action = SQL_script.strip().split()[0]  # Obtener la acción (CREATE, SELECT, etc.)
        table_name = "animum-dev-datawarehouse.tp_02st_01.hs_negocios_tutor_to_contact_cleaned"
        
        # Mostrar un resumen del script SQL
        print(f"[INFO]: Acción detectada en el script SQL: {action}")
        print("[INFO]: Resumen del script SQL:")
        for line in SQL_script.strip().split('\n')[:5]:  # Mostrar las primeras 5 líneas
            print(line)
        print("...")
        
        print("[INFO]: Ejecutando el script SQL...")
        query_job = GBQclient.query(SQL_script)
        print("[INFO]: Tiempo de inicio: %s" % time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(start_time)))
        
        # Esperar a que se complete la consulta
        result = query_job.result()
        elapsed_time = time.time() - start_time
        
        print("[SUCCESS]: Consulta SQL ejecutada exitosamente.\n")
        
        # Verificar las filas insertadas en la tabla de destino
        count_query = f"SELECT COUNT(*) AS total_rows FROM `{table_name}`"
        count_result = GBQclient.query(count_query).result()
        rows_in_table = [row['total_rows'] for row in count_result][0]
        
        # Recoger detalles del trabajo
        rows_affected = query_job.num_dml_affected_rows if query_job.num_dml_affected_rows else 0
        job_id = query_job.job_id
        job_state = query_job.state
        creation_time = query_job.created
        total_bytes_processed = query_job.total_bytes_processed if query_job.total_bytes_processed else 0

        print("[INFO]: Detalles del trabajo:")
        print(f"- ID del trabajo: {job_id}")
        print(f"- Estado del trabajo: {job_state}")
        print(f"- Tiempo de creación: {creation_time}")
        print(f"- Bytes procesados: {total_bytes_processed}")
        print(f"- Filas afectadas reportadas: {rows_affected}")
        print(f"- Filas insertadas en la tabla de destino: {rows_in_table}")
        print(f"[INFO]: Tiempo total de ejecución: {elapsed_time:.2f} segundos\n")
        
        # Verificar detalles de almacenamiento (opcional)
        table_ref = GBQclient.get_table(table_name)
        table_size_bytes = table_ref.num_bytes / 1024  # Convertir a KB
        print(f"[INFO]: Tamaño actual de la tabla destino: {table_size_bytes:.2f} KB")
        
    except GoogleAPIError as e:
        print(f"[ERROR]: Ocurrió un error al ejecutar el script SQL: {str(e)}\n")
        return