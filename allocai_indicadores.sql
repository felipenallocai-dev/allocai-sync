-- ============================================================
-- AllocAI — Queries de Indicadores de Produtividade
-- Fonte: daily_presence (Secullum) + procedures (NefroCloud)
-- Duração padrão: HDI=4h, HDP=10h, HDCP=12h, DP=10h
-- Base produtiva: 8.8h/plantão (12h - 1h almoço - 20% perdas)
-- ============================================================

-- ─── CONSTANTES ─────────────────────────────────────────────
-- duração por tipo (em minutos):
-- HDI=240, HDP=600, HDCP=720, DP=600
-- base produtiva = 528 min (8.8h)
-- SLA urgência = 120 min (2h)


-- ============================================================
-- 1. UTILIZAÇÃO DO PLANTÃO POR TÉCNICO
-- Σ horas de procedimentos realizados / 8.8h
-- Separa utilização normal vs hora extra:
-- dias além de 14 no mês no daily_presence = hora extra (regime 12x36)
-- ============================================================
WITH dias_rank AS (
    -- ordena dias presentes por tecnico por mes; > 14 = hora extra
    SELECT
        technician_id,
        date,
        ROW_NUMBER() OVER (
            PARTITION BY technician_id, DATE_TRUNC('month', date)
            ORDER BY date
        ) AS dia_no_mes
    FROM daily_presence
    WHERE status = 'presente'
),
horas_proc AS (
    SELECT
        p.technician1 AS tecnico_nome,
        dp.date,
        dp.shift,
        dp.departamento,
        CASE WHEN dr.dia_no_mes > 14 THEN 'hora_extra' ELSE 'normal' END AS tipo_dia,
        SUM(
            CASE p.procedure_type
                WHEN 'HDI'  THEN 240
                WHEN 'HDP'  THEN 600
                WHEN 'HDCP' THEN 720
                WHEN 'DP'   THEN 600
                ELSE COALESCE(
                    EXTRACT(EPOCH FROM (p.end_time::TIMESTAMP - p.start_time::TIMESTAMP)) / 60,
                    240
                )
            END
        ) AS minutos_proc
    FROM procedures p
    JOIN daily_presence dp
        ON dp.technician_id = (
            SELECT id FROM technicians WHERE name = p.technician1 LIMIT 1
        )
        AND dp.date = p.procedure_date
        AND dp.status = 'presente'
    JOIN dias_rank dr
        ON dr.technician_id = dp.technician_id
        AND dr.date = dp.date
    WHERE p.status = 'REALIZADO'
    GROUP BY p.technician1, dp.date, dp.shift, dp.departamento,
             CASE WHEN dr.dia_no_mes > 14 THEN 'hora_extra' ELSE 'normal' END

    UNION ALL

    SELECT
        p.technician2 AS tecnico_nome,
        dp.date,
        dp.shift,
        dp.departamento,
        CASE WHEN dr.dia_no_mes > 14 THEN 'hora_extra' ELSE 'normal' END AS tipo_dia,
        SUM(
            CASE p.procedure_type
                WHEN 'HDI'  THEN 240
                WHEN 'HDP'  THEN 600
                WHEN 'HDCP' THEN 720
                WHEN 'DP'   THEN 600
                ELSE COALESCE(
                    EXTRACT(EPOCH FROM (p.end_time::TIMESTAMP - p.start_time::TIMESTAMP)) / 60,
                    240
                )
            END
        ) AS minutos_proc
    FROM procedures p
    JOIN daily_presence dp
        ON dp.technician_id = (
            SELECT id FROM technicians WHERE name = p.technician2 LIMIT 1
        )
        AND dp.date = p.procedure_date
        AND dp.status = 'presente'
    JOIN dias_rank dr
        ON dr.technician_id = dp.technician_id
        AND dr.date = dp.date
    WHERE p.status = 'REALIZADO'
      AND p.technician2 IS NOT NULL
    GROUP BY p.technician2, dp.date, dp.shift, dp.departamento,
             CASE WHEN dr.dia_no_mes > 14 THEN 'hora_extra' ELSE 'normal' END
)
SELECT
    tecnico_nome,
    departamento,
    tipo_dia,
    COUNT(*) AS dias,
    ROUND(AVG(minutos_proc)) AS media_min_proc,
    ROUND(AVG(minutos_proc) / 528.0 * 100, 1) AS util_plantao_pct,
    ROUND(AVG(528.0 - minutos_proc)) AS ociosidade_media_min
FROM horas_proc
GROUP BY tecnico_nome, departamento, tipo_dia
ORDER BY tecnico_nome, departamento, tipo_dia;


-- ============================================================
-- 2. SCORE DE PRESENÇA EFETIVA POR TÉCNICO
-- Presença = dias presente / dias esperados
-- ============================================================
SELECT
    t.name AS tecnico,
    dp.departamento,
    dp.shift AS turno,
    -- dias_esperados: exclui folga/ferias (dias que o tecnico nao deveria trabalhar)
    -- ~14 dias/mes por tecnico em regime de plantao 12x36
    SUM(CASE WHEN dp.status NOT IN ('folga','ferias') THEN 1 ELSE 0 END) AS dias_esperados,
    SUM(CASE WHEN dp.status = 'presente' THEN 1 ELSE 0 END) AS dias_presente,
    SUM(CASE WHEN dp.status = 'falta' THEN 1 ELSE 0 END) AS faltas,
    SUM(CASE WHEN dp.status = 'atestado' THEN 1 ELSE 0 END) AS atestados,
    SUM(CASE WHEN dp.status = 'ausente' THEN 1 ELSE 0 END) AS ausencias,
    ROUND(
        SUM(CASE WHEN dp.status = 'presente' THEN 1 ELSE 0 END)::NUMERIC
        / NULLIF(SUM(CASE WHEN dp.status NOT IN ('folga','ferias') THEN 1 ELSE 0 END), 0) * 100, 1
    ) AS presenca_pct
FROM daily_presence dp
JOIN technicians t ON t.id = dp.technician_id
WHERE dp.date BETWEEN '2026-01-01' AND '2026-05-15'
GROUP BY t.name, dp.departamento, dp.shift
ORDER BY presenca_pct DESC;


-- ============================================================
-- 3. TAXA DE SUSPENSÃO POR HOSPITAL
-- ============================================================
SELECT
    h.name AS hospital,
    COUNT(*) AS total_proc,
    SUM(CASE WHEN p.status = 'REALIZADO' THEN 1 ELSE 0 END) AS realizados,
    SUM(CASE WHEN p.status = 'SUSPENSO' THEN 1 ELSE 0 END) AS suspensos,
    SUM(CASE WHEN p.status = 'CANCELADO' THEN 1 ELSE 0 END) AS cancelados,
    ROUND(
        SUM(CASE WHEN p.status = 'SUSPENSO' THEN 1 ELSE 0 END)::NUMERIC
        / NULLIF(COUNT(*), 0) * 100, 1
    ) AS taxa_suspensao_pct,
    ROUND(
        SUM(CASE WHEN p.status = 'REALIZADO' THEN 1 ELSE 0 END)::NUMERIC
        / NULLIF(COUNT(*), 0) * 100, 1
    ) AS taxa_realizacao_pct
FROM procedures p
JOIN hospitals h ON h.id = p.hospital_id
GROUP BY h.name
ORDER BY taxa_suspensao_pct DESC;


-- ============================================================
-- 4. PADRÃO DE SUSPENSÃO POR DIA DA SEMANA
-- ============================================================
SELECT
    TO_CHAR(p.procedure_date, 'Day') AS dia_semana,
    EXTRACT(DOW FROM p.procedure_date) AS dow,
    COUNT(*) AS total,
    SUM(CASE WHEN p.status = 'SUSPENSO' THEN 1 ELSE 0 END) AS suspensos,
    ROUND(
        SUM(CASE WHEN p.status = 'SUSPENSO' THEN 1 ELSE 0 END)::NUMERIC
        / NULLIF(COUNT(*), 0) * 100, 1
    ) AS taxa_suspensao_pct
FROM procedures p
GROUP BY dia_semana, dow
ORDER BY dow;


-- ============================================================
-- 5. SUSPENSÃO TARDIA
-- Procedimento suspenso depois que o turno começou
-- ============================================================
SELECT
    p.procedure_date,
    h.name AS hospital,
    p.patient_name,
    p.procedure_type,
    p.shift,
    p.start_time AS horario_previsto,
    p.suspension_reason,
    p.technician1,
    p.technician2
FROM procedures p
JOIN hospitals h ON h.id = p.hospital_id
WHERE p.status = 'SUSPENSO'
  AND p.start_time IS NOT NULL
ORDER BY p.procedure_date DESC, p.start_time;


-- ============================================================
-- 6. GAP PRESCRIÇÃO → INÍCIO (SLA urgência/emergência)
-- Meta: < 120 min para urgências e emergências
-- ============================================================
SELECT
    h.name AS hospital,
    p.classification,
    COUNT(*) AS total,
    ROUND(AVG(
        EXTRACT(EPOCH FROM (
            p.procedure_date + p.start_time::INTERVAL
            - p.prescription_datetime
        )) / 60
    )) AS gap_medio_min,
    ROUND(AVG(
        EXTRACT(EPOCH FROM (
            p.procedure_date + p.start_time::INTERVAL
            - p.prescription_datetime
        )) / 60
    ) / 60.0, 1) AS gap_medio_horas,
    SUM(CASE
        WHEN EXTRACT(EPOCH FROM (
            p.procedure_date + p.start_time::INTERVAL
            - p.prescription_datetime
        )) / 60 > 120 THEN 1 ELSE 0
    END) AS fora_sla,
    ROUND(
        SUM(CASE
            WHEN EXTRACT(EPOCH FROM (
                p.procedure_date + p.start_time::INTERVAL
                - p.prescription_datetime
            )) / 60 > 120 THEN 1 ELSE 0
        END)::NUMERIC / NULLIF(COUNT(*), 0) * 100, 1
    ) AS pct_fora_sla
FROM procedures p
JOIN hospitals h ON h.id = p.hospital_id
WHERE p.prescription_datetime IS NOT NULL
  AND p.start_time IS NOT NULL
  AND p.classification IN ('Urgência','Emergência')
GROUP BY h.name, p.classification
ORDER BY gap_medio_min DESC;


-- ============================================================
-- 6b. DIAGNÓSTICO — NULOS EM prescription_date (URGÊNCIA/EMERGÊNCIA)
-- Entende por que prescription_datetime está nulo no Q6
-- ============================================================
SELECT
    classification,
    COUNT(*)                                                                 AS total,
    SUM(CASE WHEN prescription_date IS NULL THEN 1 ELSE 0 END)              AS nulos_prescricao,
    SUM(CASE WHEN start_time        IS NULL THEN 1 ELSE 0 END)              AS nulos_start_time,
    SUM(CASE WHEN prescription_date IS NOT NULL
              AND start_time        IS NOT NULL THEN 1 ELSE 0 END)          AS com_dados_completos,
    ROUND(
        SUM(CASE WHEN prescription_date IS NULL THEN 1 ELSE 0 END)::NUMERIC
        / COUNT(*) * 100, 1
    )                                                                        AS pct_nulos_prescricao
FROM procedures
WHERE classification IN ('Urgência','Emergência')
GROUP BY classification
ORDER BY classification;


-- ============================================================
-- 7. PRESCRITO VS REALIZADO POR TIPO
-- ============================================================
SELECT
    procedure_type,
    COUNT(*) AS total_prescrito,
    SUM(CASE WHEN status = 'REALIZADO' THEN 1 ELSE 0 END) AS realizados,
    SUM(CASE WHEN status = 'SUSPENSO' THEN 1 ELSE 0 END) AS suspensos,
    SUM(CASE WHEN status IN ('PENDENTE','AGENDADO') THEN 1 ELSE 0 END) AS pendentes,
    ROUND(
        SUM(CASE WHEN status = 'REALIZADO' THEN 1 ELSE 0 END)::NUMERIC
        / NULLIF(COUNT(*), 0) * 100, 1
    ) AS pct_realizado
FROM procedures
GROUP BY procedure_type
ORDER BY pct_realizado DESC;


-- ============================================================
-- 8. TÉCNICOS FANTASMA
-- Aparecem em procedures mas ausentes/sem ponto no Secullum
-- ============================================================
SELECT
    p.technician1 AS tecnico,
    p.procedure_date AS data,
    p.status AS status_proc,
    p.procedure_type,
    h.name AS hospital,
    dp.status AS status_ponto
FROM procedures p
JOIN hospitals h ON h.id = p.hospital_id
LEFT JOIN daily_presence dp ON
    dp.technician_id = (
        SELECT id FROM technicians WHERE name = p.technician1 LIMIT 1
    )
    AND dp.date = p.procedure_date
WHERE p.status = 'REALIZADO'
  AND (
      dp.id IS NULL
      OR dp.status IN ('ausente', 'falta', 'folga', 'ferias')
  )
ORDER BY p.procedure_date DESC;


-- ============================================================
-- 9. OCIOSIDADE PÓS-SUSPENSÃO
-- Técnico alocado no dia com procedimento suspenso
-- ============================================================
WITH suspensos_dia AS (
    SELECT
        p.procedure_date AS data,
        p.shift,
        p.technician1 AS tecnico,
        h.name AS hospital,
        CASE p.procedure_type
            WHEN 'HDI'  THEN 240
            WHEN 'HDP'  THEN 600
            WHEN 'HDCP' THEN 720
            WHEN 'DP'   THEN 600
            ELSE 240
        END AS minutos_perdidos
    FROM procedures p
    JOIN hospitals h ON h.id = p.hospital_id
    WHERE p.status = 'SUSPENSO'
      AND p.technician1 IS NOT NULL
)
SELECT
    tecnico,
    data,
    shift,
    hospital,
    minutos_perdidos,
    ROUND(minutos_perdidos / 60.0, 1) AS horas_perdidas
FROM suspensos_dia
ORDER BY minutos_perdidos DESC;


-- ============================================================
-- 10. DEMANDA VS CAPACIDADE POR TURNO/DATA
-- ============================================================
WITH demanda AS (
    SELECT
        procedure_date AS data,
        -- normaliza turnos: procedures usa SD/SN, daily_presence usa dia/noite
        CASE work_shift
            WHEN 'SD' THEN 'dia'
            WHEN 'SN' THEN 'noite'
            ELSE lower(COALESCE(work_shift, 'sem_turno'))
        END AS turno,
        COUNT(*) AS procs_demandados
    FROM procedures
    WHERE status IN ('PENDENTE', 'AGENDADO', 'ANDAMENTO', 'REALIZADO')
    GROUP BY procedure_date,
        CASE work_shift
            WHEN 'SD' THEN 'dia'
            WHEN 'SN' THEN 'noite'
            ELSE lower(COALESCE(work_shift, 'sem_turno'))
        END
),
capacidade AS (
    SELECT
        date AS data,
        shift AS turno,
        COUNT(*) AS tecnicos_presentes
    FROM daily_presence
    WHERE status = 'presente'
    GROUP BY date, shift
)
SELECT
    d.data,
    d.turno,
    d.procs_demandados,
    COALESCE(c.tecnicos_presentes, 0) AS tecnicos_presentes,
    ROUND(
        d.procs_demandados::NUMERIC
        / NULLIF(c.tecnicos_presentes, 0) * 100, 1
    ) AS ocupacao_pct,
    d.procs_demandados - COALESCE(c.tecnicos_presentes, 0) AS gap
FROM demanda d
LEFT JOIN capacidade c ON c.data = d.data AND c.turno = d.turno
ORDER BY d.data DESC, d.turno;


-- ============================================================
-- 11. ABSENTEÍSMO POR DEPARTAMENTO E TURNO
-- ============================================================
SELECT
    departamento,
    shift AS turno,
    SUM(CASE WHEN status NOT IN ('folga','ferias') THEN 1 ELSE 0 END) AS dias_esperados,
    SUM(CASE WHEN status = 'presente' THEN 1 ELSE 0 END) AS presentes,
    SUM(CASE WHEN status = 'falta' THEN 1 ELSE 0 END) AS faltas,
    SUM(CASE WHEN status = 'atestado' THEN 1 ELSE 0 END) AS atestados,
    SUM(CASE WHEN status = 'ausente' THEN 1 ELSE 0 END) AS ausentes,
    ROUND(
        (1 - SUM(CASE WHEN status = 'presente' THEN 1 ELSE 0 END)::NUMERIC
        / NULLIF(SUM(CASE WHEN status NOT IN ('folga','ferias') THEN 1 ELSE 0 END), 0)) * 100, 1
    ) AS pct_absenteismo
FROM daily_presence
WHERE date BETWEEN '2026-01-01' AND '2026-05-15'
GROUP BY departamento, shift
ORDER BY pct_absenteismo DESC;


-- ============================================================
-- 11b. TOP 20 TÉCNICOS COM MAIOR ABSENTEÍSMO
-- ============================================================
SELECT
    t.name AS tecnico,
    dp.departamento,
    SUM(CASE WHEN dp.status NOT IN ('folga','ferias') THEN 1 ELSE 0 END) AS dias_esperados,
    SUM(CASE WHEN dp.status = 'presente'              THEN 1 ELSE 0 END) AS presentes,
    SUM(CASE WHEN dp.status NOT IN ('presente','folga','ferias') THEN 1 ELSE 0 END) AS dias_ausentes,
    ROUND(
        SUM(CASE WHEN dp.status NOT IN ('presente','folga','ferias') THEN 1 ELSE 0 END)::NUMERIC
        / NULLIF(SUM(CASE WHEN dp.status NOT IN ('folga','ferias') THEN 1 ELSE 0 END), 0) * 100, 1
    ) AS pct_absenteismo
FROM daily_presence dp
JOIN technicians t ON t.id = dp.technician_id
WHERE dp.date BETWEEN '2026-01-01' AND '2026-05-15'
GROUP BY t.name, dp.departamento
HAVING SUM(CASE WHEN dp.status NOT IN ('folga','ferias') THEN 1 ELSE 0 END) > 0
ORDER BY pct_absenteismo DESC
LIMIT 20;


-- ============================================================
-- 11c. ABSENTEÍSMO POR MÊS (TENDÊNCIA JAN-MAI 2026)
-- ============================================================
SELECT
    TO_CHAR(date, 'YYYY-MM') AS mes,
    SUM(CASE WHEN status NOT IN ('folga','ferias') THEN 1 ELSE 0 END) AS dias_esperados,
    SUM(CASE WHEN status = 'presente'              THEN 1 ELSE 0 END) AS presentes,
    SUM(CASE WHEN status = 'falta'                 THEN 1 ELSE 0 END) AS faltas,
    SUM(CASE WHEN status = 'atestado'              THEN 1 ELSE 0 END) AS atestados,
    SUM(CASE WHEN status = 'ausente'               THEN 1 ELSE 0 END) AS ausentes,
    ROUND(
        (1 - SUM(CASE WHEN status = 'presente' THEN 1 ELSE 0 END)::NUMERIC
        / NULLIF(SUM(CASE WHEN status NOT IN ('folga','ferias') THEN 1 ELSE 0 END), 0)) * 100, 1
    ) AS pct_absenteismo
FROM daily_presence
WHERE date BETWEEN '2026-01-01' AND '2026-05-15'
GROUP BY TO_CHAR(date, 'YYYY-MM')
ORDER BY mes;


-- ============================================================
-- 11d. FALTA VS ATESTADO VS AUSENTE POR DEPARTAMENTO
-- ============================================================
SELECT
    departamento,
    SUM(CASE WHEN status NOT IN ('folga','ferias') THEN 1 ELSE 0 END)  AS dias_esperados,
    SUM(CASE WHEN status = 'falta'    THEN 1 ELSE 0 END)               AS faltas,
    ROUND(SUM(CASE WHEN status = 'falta'    THEN 1 ELSE 0 END)::NUMERIC
        / NULLIF(SUM(CASE WHEN status NOT IN ('folga','ferias') THEN 1 ELSE 0 END),0)*100,1) AS pct_falta,
    SUM(CASE WHEN status = 'atestado' THEN 1 ELSE 0 END)               AS atestados,
    ROUND(SUM(CASE WHEN status = 'atestado' THEN 1 ELSE 0 END)::NUMERIC
        / NULLIF(SUM(CASE WHEN status NOT IN ('folga','ferias') THEN 1 ELSE 0 END),0)*100,1) AS pct_atestado,
    SUM(CASE WHEN status = 'ausente'  THEN 1 ELSE 0 END)               AS ausentes,
    ROUND(SUM(CASE WHEN status = 'ausente'  THEN 1 ELSE 0 END)::NUMERIC
        / NULLIF(SUM(CASE WHEN status NOT IN ('folga','ferias') THEN 1 ELSE 0 END),0)*100,1) AS pct_ausente
FROM daily_presence
WHERE date BETWEEN '2026-01-01' AND '2026-05-15'
GROUP BY departamento
ORDER BY departamento;


-- ============================================================
-- 11e. LISTA NOMINAL — ABSENTEÍSMO ACIMA DE 30%
-- ============================================================
SELECT
    t.name AS tecnico,
    dp.departamento,
    dp.shift AS turno,
    SUM(CASE WHEN dp.status NOT IN ('folga','ferias') THEN 1 ELSE 0 END) AS dias_esperados,
    SUM(CASE WHEN dp.status = 'presente'              THEN 1 ELSE 0 END) AS presentes,
    SUM(CASE WHEN dp.status = 'falta'                 THEN 1 ELSE 0 END) AS faltas,
    SUM(CASE WHEN dp.status = 'atestado'              THEN 1 ELSE 0 END) AS atestados,
    SUM(CASE WHEN dp.status = 'ausente'               THEN 1 ELSE 0 END) AS ausentes,
    ROUND(
        (1 - SUM(CASE WHEN dp.status = 'presente' THEN 1 ELSE 0 END)::NUMERIC
        / NULLIF(SUM(CASE WHEN dp.status NOT IN ('folga','ferias') THEN 1 ELSE 0 END), 0)) * 100, 1
    ) AS pct_absenteismo
FROM daily_presence dp
JOIN technicians t ON t.id = dp.technician_id
WHERE dp.date BETWEEN '2026-01-01' AND '2026-05-15'
GROUP BY t.name, dp.departamento, dp.shift
HAVING (1 - SUM(CASE WHEN dp.status = 'presente' THEN 1 ELSE 0 END)::NUMERIC
        / NULLIF(SUM(CASE WHEN dp.status NOT IN ('folga','ferias') THEN 1 ELSE 0 END), 0)) * 100 > 30
ORDER BY pct_absenteismo DESC;


-- ============================================================
-- 12. KPI QUALIDADE DE DADOS — MATCH NEFROCLOUD × SECULLUM
-- ============================================================

-- 12a. Técnicos só no NefroCloud (proc sem ponto)
SELECT DISTINCT
    p.technician1 AS tecnico,
    'Só NefroCloud' AS situacao,
    'procedimento sem ponto registrado' AS descricao
FROM procedures p
WHERE p.technician1 IS NOT NULL
  AND NOT EXISTS (
      -- unaccent() ignora diferencas de acentuacao entre os dois sistemas
      -- fallback: se unaccent nao estiver disponivel, o script Python usa regexp_replace
      SELECT 1 FROM technicians t
      WHERE lower(unaccent(t.name)) = lower(unaccent(p.technician1))
  )

UNION ALL

-- 12b. Técnicos só no Secullum (ponto sem proc)
SELECT DISTINCT
    t.name AS tecnico,
    'Só Secullum' AS situacao,
    'ponto sem procedimento vinculado' AS descricao
FROM technicians t
WHERE NOT EXISTS (
    SELECT 1 FROM procedures p
    WHERE lower(unaccent(p.technician1)) = lower(unaccent(t.name))
       OR lower(unaccent(p.technician2)) = lower(unaccent(t.name))
)
ORDER BY situacao, tecnico;


-- ============================================================
-- 13. CONCENTRAÇÃO DE ALOCAÇÕES
-- Quais técnicos respondem por 80% dos procedimentos
-- ============================================================
WITH procs_por_tecnico AS (
    SELECT technician1 AS tecnico, COUNT(*) AS n FROM procedures WHERE status = 'REALIZADO' AND technician1 IS NOT NULL GROUP BY technician1
    UNION ALL
    SELECT technician2, COUNT(*) FROM procedures WHERE status = 'REALIZADO' AND technician2 IS NOT NULL GROUP BY technician2
),
totais AS (
    SELECT tecnico, SUM(n) AS total FROM procs_por_tecnico GROUP BY tecnico
),
ranked AS (
    SELECT
        tecnico,
        total,
        SUM(total) OVER () AS grand_total,
        ROUND(total::NUMERIC / SUM(total) OVER () * 100, 1) AS pct,
        SUM(total) OVER (ORDER BY total DESC) AS running_total
    FROM totais
)
SELECT
    tecnico,
    total AS procs_realizados,
    pct AS pct_do_total,
    ROUND(running_total::NUMERIC / grand_total * 100, 1) AS pct_acumulado
FROM ranked
ORDER BY total DESC;


-- ============================================================
-- 14. HORA EXTRA — CAUSA RAIZ ESTIMADA
-- Classifica baseado no motivo de suspensão e padrão
-- ============================================================
SELECT
    CASE
        WHEN p.suspension_reason ILIKE '%manut%' THEN 'Manutenção'
        WHEN p.suspension_reason ILIKE '%medico%' OR p.suspension_reason ILIKE '%médico%' THEN 'Problema médico'
        WHEN p.suspension_reason ILIKE '%hospital%' THEN 'Problema hospital'
        WHEN p.suspension_reason ILIKE '%escala%' OR p.suspension_reason ILIKE '%alocaç%' THEN 'Erro de escala'
        WHEN p.suspension_reason ILIKE '%comunicaç%' THEN 'Comunicação'
        WHEN p.suspension_reason IS NULL THEN 'Não informado'
        ELSE 'Outros'
    END AS causa,
    COUNT(*) AS ocorrencias,
    ROUND(COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER () * 100, 1) AS pct
FROM procedures p
WHERE p.status = 'SUSPENSO'
GROUP BY causa
ORDER BY ocorrencias DESC;
