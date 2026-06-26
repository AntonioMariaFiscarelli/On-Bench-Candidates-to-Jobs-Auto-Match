WITH dati_sospensione AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY dbsospcandno
                   ORDER BY load_datetimestamp DESC
               ) AS rn2
        FROM it_discovery.consumer_int_model.PB_FTCANDTISOSPENSIONEDISP_CLEANED
    )
    WHERE rn2 = 1
),

cte_candidato AS (
    SELECT
        ca.dbcandno AS Candidate_Id,
        ca.dbAssTINoMission,
        ca.dbAssTIDataCessaz,
        d.dbSospDescGiust AS desc_giustificativo,
        j.dbjobordno AS jofilled
    FROM it_discovery.consumer_int_model.PB_FTCANDIDATE_CLEANED ca

    LEFT JOIN dati_sospensione d
        ON ca.dbcandno = d.dbSospCandNo
       AND (d.dbSospDataFine >= CURRENT_DATE OR d.dbSospDataFine IS NULL)
       AND (d.dbSospDataInizio <= CURRENT_DATE OR d.dbSospDataInizio IS NULL)
       AND d.dbSospDescGiust NOT ILIKE 'mal.no%'

    LEFT JOIN it_discovery.consumer_int_model.PB_FTJOBORDER_CLEANED j
        ON j.dbjobcandno = ca.dbcandno
       AND j.dbjobstatus IN (2,4)
       AND j.dbjobenddate >= CURRENT_DATE
       AND j.dbjobdatecommence <= CURRENT_DATE

    WHERE ca.dbAssTINoMission IS NOT NULL
      AND ca.dbAssTINoMission <= DATEADD(MONTH, 6, CURRENT_DATE)
),

candidate_onbench AS (
    SELECT DISTINCT
        c.Candidate_Id AS candidate_id_pb,
        c2.bhid AS candidate_id_bh,

        CASE
            WHEN c.dbAssTIDataCessaz <= CURRENT_DATE THEN 'Cessato'
            WHEN c.dbAssTINoMission > CURRENT_DATE THEN 'Futuro no mission'
            WHEN c.dbAssTINoMission <= CURRENT_DATE
                 AND c.jofilled IS NULL
                 AND c.desc_giustificativo IS NULL
                THEN 'No mission'
            WHEN c.dbAssTINoMission <= CURRENT_DATE
                 AND c.jofilled > 0
                THEN 'Ricollocato'
            WHEN c.dbAssTINoMission < CURRENT_DATE
                 AND c.desc_giustificativo IS NOT NULL
                THEN 'Indisponibile'
            ELSE 'da controllare'
        END AS status_candidato

    FROM cte_candidato c

    LEFT JOIN it_discovery.consumer_int_model.PB_MATRIX_CANDIDATE_CLEANED c2
        ON c.Candidate_Id = c2.MOID
),

candidates AS (
    SELECT
        pb_id,
        skill_set,
        education_degree,
        degree_list,
        description
    FROM it_discovery.consumer_int_model.candidate_cleaned
    WHERE isdeleted = 0
      AND is_anonymized = 0
),

ranked AS (
    SELECT
        dbjobordno,
        dbjobcandno,
        dbjobsite,
        dbjobposition,
        dbjobdatecommence,
        dbjobenddate,
        dbiparttimepcnt,
        ROW_NUMBER() OVER (
            PARTITION BY dbjobcandno
            ORDER BY dbjobdatecommence
        ) AS rn_first,
        ROW_NUMBER() OVER (
            PARTITION BY dbjobcandno
            ORDER BY dbjobdatecommence DESC
        ) AS rn_last
    FROM it_discovery.consumer_int_model.pb_ftjoborder_cleaned
),

jobs_ranked AS (
    SELECT
        dbjobordno,
        dbjobcandno,
        dbjobsite,
        dbjobposition,
        dbjobdatecommence,
        dbjobenddate,
        dbiparttimepcnt,
        CASE
            WHEN rn_first = 1 THEN 'first'
            WHEN rn_last = 1 THEN 'last'
            ELSE 'intermediate'
        END AS job_rank
    FROM ranked
),

jobs_ranked_info AS (
    SELECT
        jo.dbjobordno,
        jo.dbjobcandno,
        jo.job_rank,
        jo.dbjobsite,
        jo.dbjobposition,
        jo.dbjobdatecommence,
        jo.dbjobenddate,
        jo.dbiparttimepcnt,
        je.dbilevel,
        je.dbiqualification,
        ccnl_codice,
        ccnl_descrizione
    FROM jobs_ranked jo
    INNER JOIN it_discovery.consumer_int_model.pb_ftjobext_cleaned je
        ON jo.dbjobordno = je.dbjobordno
    LEFT JOIN it_discovery.consumer_int_model.v_ppccnlsalias cc
        ON cc.chiave = je.ccnlid
),

jobs_ranked_agg AS (
    SELECT
        dbjobcandno,

        MAX(CASE WHEN job_rank = 'first' THEN dbjobsite END) AS job_site,
        MAX(CASE WHEN job_rank = 'first' THEN dbjobposition END) AS job_first,

        NULLIF(
            LISTAGG(
                CASE WHEN job_rank = 'intermediate' THEN dbjobposition END,
                ', '
            ) WITHIN GROUP (ORDER BY dbjobdatecommence),
            ''
        ) AS job_intermediate,

        MAX(CASE WHEN job_rank = 'last' THEN dbjobposition END) AS job_last,

        MIN(dbjobdatecommence) AS start_date,
        MAX(dbjobenddate) AS end_date,

        MAX(CASE WHEN job_rank = 'first' THEN dbiparttimepcnt END) AS parttime_perc,
        MAX(CASE WHEN job_rank = 'first' THEN dbilevel END) AS ccnl_level,
        MAX(CASE WHEN job_rank = 'first' THEN dbiqualification END) AS legal_category,
        MAX(CASE WHEN job_rank = 'first' THEN ccnl_codice END) AS ccnl_code,
        MAX(CASE WHEN job_rank = 'first' THEN ccnl_descrizione END) AS ccnl_description

    FROM jobs_ranked_info
    GROUP BY dbjobcandno
)

SELECT
    cob.candidate_id_bh,
    jri.*,
    cob.status_candidato AS status,
    cc.skill_set AS skills,
    cc.education_degree AS education,
    cc.degree_list AS certifications,
    cc.description
FROM candidates cc
INNER JOIN candidate_onbench cob
    ON cc.pb_id = cob.candidate_id_pb
LEFT JOIN jobs_ranked_agg jri
    ON cc.pb_id = jri.dbjobcandno
where status = 'No mission'