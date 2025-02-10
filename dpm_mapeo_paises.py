# !pip install googletrans==4.0.0-rc1

# @title LISTAS DE PAISES
countries_raw = [
    "Afganist??n", "??????", "\"Selecciona tu país\"", "1", "Afganistán", "Albania", "alemania", "algeria",
    "Andorra", "Angola", "Anguila", "Antártida", "Antigua y Barbuda", "Antillas Neerlandesas", "Arabia Saudí",
    "Arabia Saudita", "Argelia", "Argentima", "argentina", "Armenia", "Aruba", "Australia", "Austria", "azerbaijan",
    "Azerbaiy??n", "Azerbaiyán", "B?©lgica", "Bahamas", "Bahr?©in", "bahrain", "Bahrein", "Bahréin", "Bangladesh",
    "Barbados", "Baréin", "Bélgica", "belgium", "Bermudas", "Bielorrusia", "Bogotá", "bolivia", "Bolivia ",
    "Botsuana", "Brasil", "brazil", "Bulgaria", "Burkina Faso", "Bután", "Cabo Verde", "California", "Camer??n",
    "cameroon", "Camerún", "Canad??", "Canada", "Canadá", "Chad", "chile", "China", "Ciudad del Vaticano",
    "colombia", "Colombia ", "Colomibia", "Comoras", "contacto", "Coombia", "Corea del Norte", "Corea del Sur",
    "Costa de Marfil", "Costa Rica", "Croacia", "Cuba", "Cusco", "czech republic", "denmark", "Desconocido",
    "Dinamarca", "Dominica", "dominican republic", "ecuador", "Ecudaor", "eeuu", "Egipto", "egypt", "El Salvador",
    "Emiratos Árabes Unidos", "equatorial guinea", "Eritrea", "ES", "Eslovaquia", "Eslovenia", "Espa", "espana",
    "españa", "Estados Unidos", "Estados Unidos (U.S.A.)", "Estados. Unidos", "Estambul", "Estonia", "Etiopía",
    "Filipinas", "finland", "Finlandia", "france", "Francia", "Gambia", "Georgia", "Germany", "ghana", "Gibraltar",
    "Granada", "Grecia", "greece", "Groenlandia", "Guadalupe", "guatemala", "Guayana Francesa", "guinea",
    "Guinea Ecuatorial", "Ha?ti", "Holanda", "honduras", "hong kong", "hungary", "Hungría", "iceland", "India",
    "Indonesia", "inglaterra", "Ir??n", "Iran", "Irán", "iran (islamic republic of)", "Iraq", "Ireland", "Irlanda",
    "Isla Bouvet", "Isla Norfolk", "Islandia", "Islas ?land", "Islas Caimán", "Islas Feroe", "Islas Heard y McDonald",
    "Islas menores alejadas de los Estados Un", "Islas Salom??n", "Islas Turcas y Caicos", "Islas Vírgenes Británicas",
    "Israel", "italia", "italy", "Jap??n", "japan", "Japon", "Japón", "Jersey UK", "jordan", "Jordania", "Kazajistán",
    "kenya", "Kiribati", "kuwait", "Laos", "Latino", "latvia", "lebanon", "Letonia", "Líbano", "Liberia",
    "Liechtenstein", "Lima", "lithuania", "luxembourg", "Luxemburgo", "Macedonia", "Málaga", "Mali", "Malta",
    "Marruecos", "Mauricio", "Mejico", "Méjico", "mexico", "México", "Mëxico", "Mexico City", "Miami", "Micronesia",
    "Mónaco", "Mongolia", "morocco", "Myanmar", "Nepal", "netherlands", "new zealand", "Nicaragua", "Nigeria",
    "no aporta", "Noruega", "norway", "Nueva Zelanda", "País", "Paises Bajos", "Países Bajos", "Pakistan", "Pakistán",
    "Palestina", "Panam??", "Panama", "Panamá", "paraguay", "peru", "Perú", "philippines", "Pitcairn", "poland",
    "Polonia", "Portugal", "Puerto Rico", "Qatar", "Regi??n Administrativa Especial de Macao", "Región desconocida o no válida",
    "Reino Unido", "Rep??blica Dominicana", "Rep. Dominicana", "República Checa", "República Dominciana",
    "República Dominicana", "romania", "Rumania", "Rumanía", "Rusia", "russian federation", "Sáhara Occidental",
    "Samoa", "San Bartolomé", "San Martín", "San Vicente y las Granadinas", "Santo Domingo", "saudi arabia",
    "Selecciona tu país", "Senegal", "Serbia", "Singapore", "Singapur", "Siria", "slovakia (slovak republic)",
    "slovenia", "Somalia", "south korea", "spain", "Suazilandia", "Sud??frica", "Sud??n", "Suecia", "Suiza", "sweden",
    "switzerland", "Taiw??n", "Taiwán", "taiwan province of china", "Territorios Australes Franceses", "Thailand",
    "Tonga", "Trinidad y Tobago", "Túnez", "Tunisia", "turkey", "Turquía", "Ucrania", "ukraine", "united arab emirates",
    "united kingdom", "united states", "Uruguay", "USA", "uzbekistan", "Uzbekistán", "venezuela", "Vi?t Nam",
    "Wallis y Futuna", "Yemen", "Yibuti", "Zambia", "Zimbabue"
]

# Lista oficial de países
official_countries = [
"afghanistan",
"åland islands",
"albania",
"algeria",
"american samoa",
"andorra",
"angola",
"anguilla",
"antarctica",
"antigua and barbuda",
"argentina",
"armenia",
"aruba",
"australia",
"austria",
"azerbaijan",
"bahamas",
"bahrain",
"bangladesh",
"barbados",
"belarus",
"belgium",
"belize",
"benin",
"bermuda",
"bhutan",
"bolivia",
"bonaire, sint eustatius and saba",
"bosnia and herzegovina",
"botswana",
"bouvet island",
"brazil",
"british indian ocean territory",
"brunei darussalam",
"bulgaria",
"burkina faso",
"burundi",
"cabo verde",
"cambodia",
"cameroon",
"canada",
"cayman islands",
"central african republic",
"chad",
"chile",
"chinese",
"christmas island",
"cocos (keeling) islands",
"colombia",
"comoros",
"congo",
"congo, democratic republic of the",
"cook islands",
"costa rica",
"côte d'ivoire",
"croatia",
"cuba",
"curaçao",
"cyprus",
"czech republic",
"denmark",
"djibouti",
"dominica",
"dominican republic",
"ecuador",
"egypt",
"el salvador",
"equatorial guinea",
"eritrea",
"estonia",
"eswatini",
"ethiopia",
"falkland islands (malvinas)",
"faroe islands",
"fiji",
"finland",
"france",
"french guiana",
"french polynesia",
"french southern territories",
"gabon",
"gambia",
"georgia",
"germany",
"ghana",
"gibraltar",
"greece",
"greenland",
"grenada",
"guadeloupe",
"guam",
"guatemala",
"guernsey",
"guinea",
"guinea-bissau",
"guyana",
"haiti",
"heard island and mcdonald islands",
"holy see",
"honduras",
"hong kong",
"hungary",
"iceland",
"india",
"indonesia",
"iran, islamic republic of",
"iran",
"iraq",
"ireland",
"isle of man",
"israel",
"italy",
"jamaica",
"netherlands",
"japan",
"jersey",
"jordan",
"kazakhstan",
"kenya",
"kiribati",
"korea, democratic people's republic of",
"korea, republic of",
"kuwait",
"kyrgyzstan",
"lao people's democratic republic",
"latvia",
"lebanon",
"lesotho",
"liberia",
"libya",
"liechtenstein",
"lithuania",
"luxembourg",
"macao",
"madagascar",
"malawi",
"malaysia",
"maldives",
"mali",
"malta",
"marshall islands",
"martinique",
"mauritania",
"mauritius",
"mayotte",
"mexico",
"micronesia, federated states of",
"moldova, republic of",
"monaco",
"mongolia",
"montenegro",
"montserrat",
"morocco",
"mozambique",
"myanmar",
"namibia",
"nauru",
"nepal",
"netherlands, kingdom of the",
"new caledonia",
"new zealand",
"nicaragua",
"niger",
"nigeria",
"niue",
"norfolk island",
"north macedonia",
"northern mariana islands",
"norway",
"oman",
"pakistan",
"palau",
"palestine, state of",
"panama",
"papua new guinea",
"paraguay",
"peru",
"philippines",
"pitcairn",
"poland",
"portugal",
"puerto rico",
"qatar",
"réunion",
"romania",
"russia",
"rwanda",
"saint barthélemy",
"saint helena, ascension and tristan da cunha",
"saint kitts and nevis",
"saint lucia",
"saint martin (french part)",
"saint pierre and miquelon",
"saint vincent and the grenadines",
"samoa",
"san marino",
"sao tome and principe",
"saudi arabia",
"senegal",
"serbia",
"seychelles",
"sierra leone",
"singapore",
"sint maarten (dutch part)",
"slovakia",
"slovenia",
"solomon islands",
"somalia",
"south africa",
"south georgia and the south sandwich islands",
"south sudan",
"spain",
"sri lanka",
"sudan",
"suriname",
"svalbard and jan mayen",
"sweden",
"swiss",
"switzerland",
"syrian arab republic",
"taiwan, province of china[note 1]",
"tajikistan",
"tanzania, united republic of",
"thailand",
"timor-leste",
"togo",
"tokelau",
"tonga",
"trinidad and tobago",
"tunisia",
"turkey",
"turkmenistan",
"turks and caicos islands",
"tuvalu",
"uganda",
"ukraine",
"united arab emirates",
"united kingdom of great britain and northern ireland",
"united states minor outlying islands",
"united states of america",
"USA",
"uruguay",
"uzbekistan",
"vanuatu",
"venezuela, bolivarian republic of",
"venezuela",
"viet nam",
"virgin islands (british)",
"virgin islands (u.s.)",
"wallis and futuna",
"western sahara",
"yemen",
"zambia",
"united kingdom",
"zimbabwe"
]

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

# @title property_country_mapped
params = {
    "elements_list_raw_value": countries_raw,  # Cambiado a "elements_list_raw_value"
    "elements_list_reference_value": official_countries,  # Cambiado a "elements_list_reference_value"
    "threshold_similarity": 0.7,
    "translate_to_english": True,  # Activar o desactivar la traducción previa al inglés
    "elements_list_raw_context_prefix": "el país llamado"  # Prefijo para añadir contexto a los valores originales antes de traducir
}

df_result = map_elements_dynamic_df(params)

# Mostrar DataFrame resultante
display(df_result)
