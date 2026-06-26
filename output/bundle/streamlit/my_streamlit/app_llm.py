# Import python packages
import streamlit as st
import warnings
#from src.autoMatch.utils.snowflake_utils import get_snowpark_session #use this locally
#from snowflake.snowpark.context import get_active_session
import datetime

from snowflake.snowpark import functions as F
from snowflake.snowpark.functions import col, lit, lower, trim, to_varchar, count_distinct
from snowflake.snowpark.functions import expr, array_size
import re
import ast

from functools import reduce
import operator

from src.onBench.utils.common import haversine
from src.onBench.utils.common import is_valid_number

from src.onBench.config.configuration import ConfigurationManager
from src.onBench.components.automatch import Automatch




st.set_page_config(layout="wide")

# Get the current credentials
#session = get_active_session()

@st.cache_resource
def get_snowpark_session_streamlit():
    # Try Snowflake first
    try:
        from snowflake.snowpark.context import get_active_session
        return get_active_session()
    except Exception:
        # Fallback to local session creation
        from src.onBench.utils.snowflake_utils import get_snowpark_session
        return get_snowpark_session()


@st.cache_resource
def get_Automatch():
    config = ConfigurationManager()
    automatch_config = config.get_automatch_config()
    automatch = Automatch(config=automatch_config)
    return automatch

@st.cache_resource
def get_app_config():
    config = ConfigurationManager()
    app_config = config.get_app_config()
    return app_config

session = get_snowpark_session_streamlit()
automatch = get_Automatch()
app_config=get_app_config()

languages = [""] + automatch.config.languages
education_levels = [""] + automatch.config.education_levels
#desired_locations = [""] + automatch.config.desired_locations
turno_preferenza = [""] + automatch.config.turno_preferenza
parttime_preferenza_perc = [0] + automatch.config.parttime_preferenza_perc 

if "vacancy" not in st.session_state:
    st.session_state.vacancy = dict({"JOBORDER": "999999"})

vacancy_temp_name = "VACANCY_APP_TEMP"


def assign_if_not_none(key, value): 
    if value is not None: 
        st.session_state[key] = value

def assign_string_if_not_none(key, value, allowed):
    if value is None:
        st.session_state[key] = ""
    else:
        allowed = set(allowed)  # the options of your multiselect

        if isinstance(value, list):
            # keep only valid values
            if len(list) == 0:
                st.session_state[key] = ""
            elif len(list) == 1:
                st.session_state[key] = value[0]
            else:
                cleaned = [a for a in allowed if a.lower() in [v.lower() for v in value]]
                if cleaned:
                    st.session_state[key] = ", ".join(cleaned)
        elif isinstance(value, str) and value.strip():
            if(value.strip()== ""):
                st.session_state[key] = ""
            else:
                parts = [v.strip().lower() for v in value.split(",")]
                cleaned = [a for a in allowed if a.lower() in parts]#[v for v in parts if v in allowed]
                if cleaned:
                    st.session_state[key] = ", ".join(cleaned)

def assign_list_if_not_none(key, value, allowed): 
    if value is None:
        st.session_state[key] = []
    else:
        allowed = set(allowed)  # the options of your multiselect

        if isinstance(value, list):
            # keep only valid values
            if len(list) == 0:
                st.session_state[key] = [""]
            elif len(list) == 1:
                st.session_state[key] = [value]
            else:
                cleaned = [a for a in allowed if a in value]
                if cleaned:
                    st.session_state[key] = cleaned
        elif isinstance(value, str) and value.strip():
            if(value.strip()== ""):
                st.session_state[key] = [""]
            else:
                parts = [v.strip().lower() for v in value.split(",")]
                cleaned = [a for a in allowed if a.lower() in parts]#[v for v in parts if v in allowed]
                if cleaned:
                    st.session_state[key] = cleaned

def set_default_values(vacancy_id):
    df_vacancy = session.sql(f"""SELECT * FROM MPG_IT_AUTOMATCH_JOBORDER_FEATURES WHERE JOBORDERID = {vacancy_id}""")
    df_vacancy.write.save_as_table(vacancy_temp_name, mode="overwrite" )
    row = df_vacancy.first()

    reset_filters(vacancy=False)
    if row is None:
        st.warning("❌ Vacancy non trovata. Controllare l'ID inserito.")
        st.session_state.vacancy = None
    else:
        data = row.as_dict()
        st.session_state.vacancy = data

        assign_if_not_none("role", data.get("JOBTITLE"))
        assign_if_not_none("location", data.get("LOCATION"))
        assign_if_not_none("region", data.get("REGION"))
        assign_if_not_none("country", data.get("COUNTRY"))

        assign_if_not_none("max_ral", data.get("SALARY_LOW"))
        assign_if_not_none("date_available", data.get("DATE_AVAILABLE"))
        #assign_if_not_none("will_relocate", data.get("WILL_RELOCATE"))
        #assign_if_not_none("desired_locations", data.get("DESIRED_LOCATION"))
        assign_list_if_not_none("turno_preferenza", data.get("TURNO_PREFERENZA"), turno_preferenza)
        assign_if_not_none("parttime_preferenza_perc", data.get("PARTTIME_PREFERENZA_PERC"))
        assign_if_not_none("skills_opt", data.get("SKILLS"))
        assign_list_if_not_none("skills_languages_opt", data.get("LANGUAGES"), languages)
        assign_string_if_not_none("skills_education_opt", data.get("EDUCATION"), education_levels)
        assign_if_not_none("skills_certifications_opt", data.get("CERTIFICATIONS"))


def update_vacancy_from_inputs():
    """
    Update st.session_state.vacancy with the values currently inserted
    by the recruiter in the Streamlit input widgets.
    """

    # If no vacancy loaded, nothing to update
    #if "vacancy" not in st.session_state or st.session_state.vacancy is None:
    #    return

    vac = st.session_state.vacancy

    def list_to_string(value):
        if isinstance(value, list):
            return ", ".join(value) if value else None   # empty list → None
        if isinstance(value, str):
            if(value == ""):
                return None
        if value is None:
            return None
        return value

    vac["JOBTITLE"] = list_to_string(st.session_state.get("role"))
    vac["LOCATION"] = list_to_string(st.session_state.get("location"))
    vac["REGION"] = list_to_string(st.session_state.get("region"))
    vac["COUNTRY"] = list_to_string(st.session_state.get("country"))

    vac["SALARY_LOW"] = st.session_state.get("max_ral")
    vac["DATE_AVAILABLE"] = st.session_state.get("date_available")
    vac["PARTTIME_PREFERENZA_PERC"] = st.session_state.get("parttime_preferenza_perc")

    # --- LIST FIELDS ---
    vac["TURNO_PREFERENZA"] = list_to_string(st.session_state.get("turno_preferenza"))
    vac["LANGUAGES"] = list_to_string(st.session_state.get("skills_languages_opt"))

    # --- STRING FIELDS ---
    vac["EDUCATION"] = list_to_string(st.session_state.get("skills_education_opt"))
    vac["CERTIFICATIONS"] = list_to_string(st.session_state.get("skills_certifications_opt"))

    # --- FREE TEXT FIELDS ---
    vac["SKILLS"] = list_to_string(st.session_state.get("skills_opt"))

    session.create_dataframe([vac]).write.save_as_table(
        vacancy_temp_name,
        mode="overwrite",
        #table_type="temporary"
    )

    # Save back into session_state
    st.session_state.vacancy = vac


def reset_filters(vacancy=True):
    for key, value in app_config.default_inputs.items():
        if(not "vacancy_id_raw" in key or vacancy):
            st.session_state[key] = value

def init_reset_inputs():
    reset = st.button("Reset filtri", on_click=reset_filters)


def search():
    vacancy_id = st.session_state.vacancy_id_raw
    with st.spinner("Recupero vacancy ed estrazione campi..."):
        #if st.session_state.get("vacancy_id_raw"):
            numeric_value = re.sub(r"\D", "", st.session_state.vacancy_id_raw)
            #st.session_state.vacancy_id = numeric_value
            if numeric_value:
                set_default_values(numeric_value)
            else:
                st.warning("Inserire un vacancy ID valido.")
        #else:
        #    st.warning("Inserire un vacancy ID valido.")

def init_vacancy_input():
    col1, col2 = st.columns([1, 1])  

    with col1:
        st.text_input(
            "Inserire vacancy ID",
            key="vacancy_id_raw",
            help="Inserire vacancy ID per l'estrazione automatica di feature",
        )

    with col2:
        st.write("") 
        st.write("")
        st.button("Cerca per vacancy ID", on_click=search)
    



def init_role_loc_dist_age_input():
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.text_input("Mansione", 
                    #value="Magazziniere", 
                    key="role",
                    help="Esempio: Magazziniere")

    with col2:
        st.text_input("Location", 
                    #value="Bari", 
                    key="location",
                    help="Città di ricerca. Esempio: Bari")

    with col3:
        if "max_distance" not in st.session_state: st.session_state.max_distance = 20
        st.number_input(
            "Distanza massima in KM",
            key="max_distance",
            #value=20,
            step=1,
            help=(
                "Distanza massima di ricerca rispetto alla città di riferimento. "
                "Tutti i candidati al di fuori del raggio specificato verranno scartati."
            )
        )

    with col4:
        if "max_age" not in st.session_state: st.session_state.max_age = 0
        st.number_input("Età massima",
                        key ="max_age", 
                        format="%d",
                        min_value=0, 
                        #value=st.session_state.max_age,#=40, 
                        step=1, 
                        # help="""Età massima. 
                        # Tutti i candidati di età superiore verrano scartati"""
                        )

def bullhorn_fields_input():
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.number_input("RAL attuale", 
                        min_value=0, step=1, 
                        key="max_ral",
                        help="""RAL attuale. 
                        Tutti i candidati con RAL superiore a quella specificate verranno scartati""")
        
    if st.session_state.get("date_available") is None: st.session_state["date_available"] = datetime.date.today()
    with col2:
        st.date_input(
            "Disponibile da",
            #value=None,#date.today(),   # default to today
            key="date_available",
            help="""Data in cui il candidato è disponibile a iniziare. 
                    I candidati con disponibilità successiva verranno scartati."""
        ) 

    if(False):
        with col3:
            st.checkbox(
                "Disponibile al trasferimento",
                key="will_relocate",
                help="""Se selezionato, verranno mostrati solo i candidati disponibili al trasferimento."""
            )
            #if st.button("Resetta data a oggi"):
            #    st.session_state.date_available = date.today()
        
        #st.session_state.setdefault("desired_locations",  desired_locations[0])
        with col4:
            st.selectbox(
                "Disponibilità geografica",
                options=desired_locations,
                key="desired_locations",
                help="""I candidati che non sono disponibili nelle zone specificate verranno scartati"""
            )

    #st.session_state.setdefault("turno_preferenza", [])
    with col3:
        st.multiselect(
            "Seleziona turni",
            options=turno_preferenza,
            key="turno_preferenza",
            help="""I candidati che non sono disponibili nei turni specificati verranno scartati"""
        )

    with col4:

        raw_value = st.session_state.get("parttime_preferenza_perc", None)
        try:
            current_value = int(raw_value)
        except:
            current_value = None

        # find closest
        if current_value is not None:
            closest_value = min(parttime_preferenza_perc, key=lambda x: abs(x - current_value))
        else:
            closest_value = parttime_preferenza_perc[0]

        # Step 3: assign back
        st.session_state.parttime_preferenza_perc = closest_value

        st.selectbox(
            "Seleziona % part-time",
            options=parttime_preferenza_perc,
            key="parttime_preferenza_perc",
            help="""I candidati con percentuale preferenza part-time inferiore a quella specificata verranno scartati"""
        )

def init_skills_input():
    col1, col2 = st.columns(2)
    with col1:
        st.text_area(
            "Skills tecniche obbligatorie (separate da virgola)",
            value="",#"Muletto",
            key="skills_mand",
            help="""Esempio: muletto, pacchetto office, gestionale inventario. 
            I candidati non in possesso di tutte le skills richieste verrano scartati"""
        )

    # metti la seconda text_area nella seconda colonna
    with col2:
        st.text_area(
            "Skills tecniche facoltative (separate da virgola)",
            #value="",#"Excel",
            key="skills_opt",
            help="""Esempio: muletto, pacchetto office, gestionale inventario. 
            Un bonus sarà assegnato per ogni skill corrispondente del candidato"""
        )

def init_skills_soft_input():
    st.session_state.skills_soft = st.text_area("Skills soft (separate da virgola)", value="")

def init_skills_languages_input():
    #st.session_state.setdefault("skills_languages_selected_mand", [])
    #st.session_state.setdefault("skills_languages_selected_opt", [])

    col1, col2 = st.columns(2)
    with col1:
        st.multiselect(
            "Seleziona lingue obbligatorie",
            options=languages,
            key="skills_languages_mand",
            help="""I candidati che non conoscono le lingue richieste verrano scartati"""
        )

    with col2:
        st.multiselect(
            "Seleziona lingue facoltative",
            options=languages,
            key="skills_languages_opt",
            help="""Un bonus sarà assegnato per ogni lingua conosciuta dal candidato tra quelle richieste"""
        )

    #st.session_state.skills_languages_mand = st.session_state["skills_languages_selected_mand"]
    #st.session_state.skills_languages_opt = st.session_state["skills_languages_selected_opt"]

def init_skills_education_input():

    #st.session_state.setdefault("skills_education_selected_mand",  education_levels[0])
    #st.session_state.setdefault("skills_education_selected_opt", education_levels[0])

    # layout: multiselect larga, checkbox stretta a destra
    col1, col2 = st.columns(2)
    with col1:
        st.selectbox(
            "Seleziona il titolo di studio minimo",
            options=education_levels,
            key="skills_education_mand",
            help="I candidati che non possiedono un titolo di studio equivalente o superiore verranno scartati"""
        )
    with col2:
        st.selectbox(
            "Seleziona titolo di studio facoltativo",
            options=education_levels,
            key="skills_education_opt",
            help="Un bonus sarà assegnato per ogni titolo di studio del candidato che risulti superiore a quello richiesto"
        )

    #st.session_state.skills_education_mand = st.session_state["skills_education_selected_mand"]
    #st.session_state.skills_education_opt = st.session_state["skills_education_selected_opt"]# if ed_opt != education_levels[0] else ""

def init_skills_certifications_input():
    col1, col2 = st.columns(2)
    with col1:
        st.text_area(
            "Certificazioni obbligatorie (separate da virgola)",
            value="",#"Muletto",
            key="skills_certifications_mand",
            help="""Esempio: patente B, ECTL. 
            I candidati che non possiedono le certificazioni richieste verranno scartati"""
        )

    with col2:
        st.text_area(
            "Certificazioni facoltative (separate da virgola)",
            value="",#"Excel",
            key="skills_certifications_opt",
            help="""Esempio: patente B, ECTL. 
            Un bonus sarà assegnato per ogni certificazione corrispondente del candidato
            """
        )

def init_limit_input():
    st.session_state.limit = st.number_input("Numero di candidati", min_value=1, value=5)


def columns_to_show():
    cols_default = ["score", "candidateid", "first_name", "last_name", "location", "url"]
    #cols.append("score_job")

    #if st.session_state.max_age:
    cols_default.append("age")

    cols_default.extend(["last_job", "second_last_job", "third_last_job"])

    cols = cols_default
    if bool(st.session_state.skills_mand):
        cols.append("skills")
        #cols.append("mand_skills")
    if bool(st.session_state.skills_opt):
        if "skills" not in cols: cols.append("skills")
        #cols.append("score_skills")
    if bool(st.session_state.skills_languages_mand):
        cols.append("languages")
        #cols.append("mand_languages")
    if bool(st.session_state.skills_languages_opt):
        if "languages" not in cols: cols.append("languages")
        #cols.append("score_languages")
    if bool(st.session_state.skills_education_mand):
        cols.append("education")
        #cols.append("mand_education")
    if bool(st.session_state.skills_education_opt):
        if "education" not in cols: cols.append("education")
        #cols.append("score_education")
    if bool(st.session_state.skills_certifications_mand):
        cols.append("certifications")
        #cols.append("mand_certifications")
    if bool(st.session_state.skills_certifications_opt):
        if "certifications" not in cols: cols.append("certifications")
        #cols.append("score_certifications")

    if bool(st.session_state.max_ral):
        cols.append("salary_low")
    if bool(st.session_state.date_available):
        cols.append("date_available")
    #if bool(st.session_state.will_relocate):
    #    cols.append("will_relocate")
    #if bool(st.session_state.desired_locations):
    #    cols.append("desired_location")
    if bool(st.session_state.turno_preferenza):
        cols.append("turno_preferenza")
    if bool(st.session_state.parttime_preferenza_perc):
        cols.append("parttime_preferenza_perc")
    

    return cols_default, cols

def filter_candidates():

    df_candidates = session.table(f"{db}.{schema}.{table}")

    provinces = ['Foggia', 'Bari', 'Andria', 'Trani', 'BAT', 'Barletta-Andria-Trani', 'Taranto', 'Lecce', 'Brindisi',
                 'Ferrara', 'Forlì-Cesena', 'Modena', 'Parma', 'Piacenza', 'Ravenna', 'Reggio Emilia', 'Rimini']
    
    provinces = [p.lower() for p in provinces]
    df_candidates = df_candidates.filter(lower(col("PROVINCE_EXT")).isin(provinces))


    filters = []
    location_filter = lower(trim(col("location"))) == st.session_state.location.strip().lower()
    distance_filter = col("distance_km") <= st.session_state.max_distance

    if st.session_state.location:
        # --- Get reference location ---
        df_location = (
            session.table("IT_DISCOVERY.CONSUMER_INT_MODEL.MPG_IT_AUTOMATCH_ITALIAN_CITIES")
            .filter(lower(trim(col("CITY_NAME"))) == st.session_state.location.strip().lower())
        )
        location_coords = df_location.collect()
        

        if not location_coords or location_coords is None:
            st.warning("Location non trovata.")
            filters.append(location_filter)
        else:
            location_dict = location_coords[0].as_dict()
            lat_ref = location_dict.get("LATITUDE")
            lon_ref = location_dict.get("LONGITUDE")
            vacancy_region = location_dict.get("REGION").strip().lower()
            region_filter = (lower(col("region")) == vacancy_region) & (col("desired_location") == lit("Regionale")) 

            if is_valid_number(lat_ref) and is_valid_number(lon_ref):
                df_candidates = df_candidates.with_column("distance_km", haversine(lat_ref, lon_ref))
                filters.append(distance_filter)
                df_candidates = df_candidates.drop("LATITUDE", "LONGITUDE")
            else:
                filters.append(location_filter)

            if not vacancy_region or vacancy_region.strip() == "":
                filters = filters
            else:
                filters.append(region_filter)


    filters.append(col("desired_location") == lit("Internazionale"))
    filters.append(
        (lower(col("country")) == lit("italia")) &
        (col("desired_location") == lit("Nazionale"))
    )
    if len(filters) > 0:
        combined_filter = reduce(operator.or_, filters)
        df_candidates = df_candidates.filter(combined_filter)

                
    if st.session_state.max_age:

        df_candidates = df_candidates.filter(
            (col("AGE") <= st.session_state.max_age) | 
            col("AGE").is_null() #| 
            #col("AGE") <= lit(0)
        )

    if st.session_state.max_ral:
        df_candidates = df_candidates.filter(
            (col("SALARY_LOW") <= st.session_state.max_ral) | 
            (col("SALARY_LOW").is_null())
        )
        if(False):
            df_candidates = df_candidates.sort(
                col("SALARY_LOW").is_null().asc()   # False (non-null) comes before True (null)
            )

    if st.session_state.date_available:
        df_candidates = df_candidates.filter(
            (col("DATE_AVAILABLE") <= lit(st.session_state.date_available)) |
            (col("DATE_AVAILABLE").is_null())
        )
        if(False):
            df_candidates = df_candidates.sort(
                col("DATE_AVAILABLE").is_null().asc(),   # non-null first
                col("DATE_AVAILABLE").asc()              # then earliest dates
            )

    if(False):
        if st.session_state.will_relocate:
            df_candidates = df_candidates.filter(
                (col("WILL_RELOCATE") == True) | 
                (col("WILL_RELOCATE").is_null())
            )
   
        if st.session_state.desired_locations:
            mapping = {level: i+1 for i, level in enumerate(desired_locations)}

            case_expr = "CASE "
            for k, v in mapping.items():
                case_expr += f"WHEN lower(trim(DESIRED_LOCATION)) = '{k.lower()}' THEN {v} "
            case_expr += "ELSE NULL END"

            df_candidates = df_candidates.with_column("loc_rank", expr(case_expr))

            threshold_pos = mapping[st.session_state.desired_locations]
            df_candidates = df_candidates.filter(col("loc_rank") >= lit(threshold_pos)) 

    if st.session_state.turno_preferenza:
        selected = st.session_state.turno_preferenza

        dfcols = df_candidates.columns

        df_no_turni = df_candidates.filter(
            (col("TURNO_PREFERENZA").is_null()) |
            (array_size(col("TURNO_PREFERENZA")) == 0)
        )

        # Flatten the array into rows
        df_flat = df_candidates.join_table_function(
            "flatten",
            col("turno_preferenza")
        ).select(
            df_candidates["*"],
            col("value").alias("turno_value")
        )

        # Keep only rows where turno_value is one of the selected values

        df_filtered = df_flat.filter(
            col("turno_value").isin(selected)
        )

        # AND logic: candidate must match ALL selected values
        df_match_all = (
            df_filtered
            .group_by("candidateid")
            .agg(count_distinct("turno_value").alias("tv"))
            .filter(col("tv") == len(selected))
        )
        # Join back to original rows
        df_si_turni = df_match_all.join(df_candidates, "candidateid")

        df_si_turni = df_si_turni.select(dfcols)
        df_no_turni = df_no_turni.select(dfcols)
        df_candidates = df_si_turni.union_all(df_no_turni)
        df_candidates = df_candidates.with_column("TURNO_PREFERENZA", to_varchar(col("TURNO_PREFERENZA")))

    if st.session_state.parttime_preferenza_perc:
        st.session_state.parttime_value = int(st.session_state.parttime_preferenza_perc)
        df_candidates = df_candidates.filter(
            (col("PARTTIME_PREFERENZA_PERC") >= st.session_state.parttime_preferenza_perc) | 
            (col("PARTTIME_PREFERENZA_PERC").is_null()) |
            (col("PARTTIME_PREFERENZA_PERC") == lit(0))
        )
        if(False):
            df_candidates = df_candidates.sort(
                col("PARTTIME_PREFERENZA_PERC").is_null().asc()   # False (non-null) comes before True (null)
            )
        

    # Skills filter
    if(True):
        if st.session_state.skills_mand.strip():
            skills_list = [s.strip().lower() for s in st.session_state.skills_mand.split(",")]
            for skill in skills_list:
                df_candidates = df_candidates.filter(lower(col("SKILLS")).like(f"%{skill}%"))

        # Skills filter
        if st.session_state.skills_certifications_mand and st.session_state.skills_certifications_mand.strip():
            skills_list = [s.strip().lower() for s in st.session_state.skills_certifications_mand.split(",")]
            for skill in skills_list:
                df_candidates = df_candidates.filter(lower(col("CERTIFICATIONS")).like(f"%{skill}%"))

    if st.session_state.skills_languages_mand:
        skills_list = [s.lower() for s in st.session_state.skills_languages_mand]
        for skill in skills_list:
            df_candidates = df_candidates.filter(lower(col("LANGUAGES")).like(f"%{skill}%"))

    if st.session_state.skills_education_mand:
        mapping = {level: i+1 for i, level in enumerate(education_levels)}

        case_expr = "CASE "
        for k, v in mapping.items():
            case_expr += f"WHEN lower(trim(EDUCATION)) = '{k.lower()}' THEN {v} "
        case_expr += "ELSE NULL END"

        df_candidates = df_candidates.with_column("edu_rank", expr(case_expr))

        threshold_pos = mapping[st.session_state.skills_education_mand]
        df_candidates = df_candidates.filter(col("edu_rank") >= lit(threshold_pos)) 


    return df_candidates


st.title("🔎 Automatch")

init_reset_inputs()

st.markdown("Inserisci l'ID della vacancy per definire automaticamente i criteri di ricerca")

init_vacancy_input()

st.subheader("Filtri di ricerca")
st.markdown("Modifica i criteri di ricerca per trovare i candidati ideali.")


init_role_loc_dist_age_input()
st.write("")
bullhorn_fields_input()
st.write("")
init_skills_input()
#init_skills_soft_input()
st.write("")
init_skills_languages_input()
init_skills_education_input()
init_skills_certifications_input()
#init_limit_input()



if st.button("Cerca candidati"):
    with st.spinner("Recupero candidati e generazione ranking AI..."):

        db = automatch.config.database
        schema = automatch.config.schema
        table = automatch.config.input_table

        columns = automatch.config.columns  

        df_candidates = filter_candidates()

        update_vacancy_from_inputs()
        #st.markdown(st.session_state.vacancy)

        # Count rows in the Snowpark DataFrame
        #candidate_count = df_candidates.count()
        
        if df_candidates.limit(1).count() == 0:#candidate_count == 0:
            st.warning("Nessun candidato trovato. Ridefinisci la ricerca")
        else:
            prompts = automatch.create_prompt_row(
                st.session_state.role,
                st.session_state.skills_mand,
                st.session_state.skills_opt,
                [], #st.session_state.skills_languages_mand,
                st.session_state.skills_languages_opt,
                [], #st.session_state.skills_education_mand,
                st.session_state.skills_education_opt,
                st.session_state.skills_certifications_mand,
                st.session_state.skills_certifications_opt
            )


            if(False):
                st.text(
                    f"{prompt}"
                    )
                for key, (pt, field) in prompts.items():
                    st.text(
                        f"{pt}"
                        )

            #st.markdown(f"{st.session_state.vacancy_id_raw}")
            top_candidates = automatch.compute_score(session, df_candidates, st.session_state.vacancy_id_raw)
            #top_candidates = automatch.call_ai_row(session, prompts, df_candidates)#.limit(100)

            print(f"top_candidates {top_candidates.shape}")
            #top_candidates = top_candidates.with_column("URL",concat(lit("https://cls70.bullhornstaffing.com/BullhornSTAFFING/OpenWindow.cfm?Entity=Candidate&id="),col("CANDIDATEID")))
            top_candidates["URL"] = (
                "https://cls70.bullhornstaffing.com/BullhornSTAFFING/OpenWindow.cfm?Entity=Candidate&id="
                + top_candidates["CANDIDATEID"].astype(str)
            )
            #top_candidates = top_candidates.sort(col("SCORE").desc())

            cols_default, cols = columns_to_show()
            #top_candidates = top_candidates.select(cols)
            cols = [s.upper() for s in cols_default]
            top_candidates = top_candidates[cols]


            top_candidates["AGE"] = top_candidates["AGE"].astype("Int64")

            if "TURNO_PREFERENZA" in top_candidates.columns:
                top_candidates["TURNO_PREFERENZA"] = (
                    top_candidates["TURNO_PREFERENZA"]
                    .apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)
                    .apply(lambda x: ", ".join(x) if isinstance(x, list) else x)
                    )

            top_candidates["URL"] = top_candidates["URL"].apply(lambda x: f"[Open Link]({x})")


            rename_map = {
                "SCORE": "Score",
                "CANDIDATEID": "ID Candidato",
                "FIRST_NAME": "Nome",
                "LAST_NAME": "Cognome",
                "LOCATION": "Location",
                "URL": "URL",
                "AGE": "Età",
                "LAST_JOB": "Ultima mansione",
                "SECOND_LAST_JOB": "Precedente mansione",
                "THIRD_LAST_JOB": "Precedente mansione2",
                "SKILLS": "Skills",
                "EDUCATION": "Titolo di Studio",
                "LANGUAGES": "Lingue",
                "CERTIFICATIONS": "Certificazioni",
                "SALARY_LOW": "RAL",
                "DATE_AVAILABLE": "Disponibile da",
                "TURNO_PREFERENZA": "Preferenza turno",
                "SALARY_LOW": "RAL",
                "PARTTIME_PREFERENZA_PERC": "Preferenza Part time %",

            }

            top_candidates = top_candidates.rename(columns=rename_map)

            top_candidates.columns = [ col.replace(" ", "\u00A0") for col in top_candidates.columns ]

            st.markdown(
                f"*Top candidates:*"
            )
            st.markdown(top_candidates.to_markdown(index=False), unsafe_allow_html=True)
