WITH email_stats AS (
  -- Статистика по електронним листам (відправлені, відкриті, переглянуті)
  SELECT
    DATE_ADD(s.date, INTERVAL es.sent_date DAY) AS date,  -- Дата відправки листа
    sp.country AS country,  -- Країна користувача
    send_interval AS send_interval,  -- Немає інтервалу для листів
    is_verified AS is_verified,  -- Листи не мають статусу верифікації
    is_unsubscribed AS is_unsubscribed,  -- Листи не мають статусу відписки
    0 AS account_cnt,  -- Листи не відображають кількість акаунтів
    COUNT(DISTINCT es.id_message) AS sent_msg,  -- Кількість надісланих листів
    COUNT(DISTINCT eo.id_message) AS open_msg,  -- Кількість відкритих листів
    COUNT(DISTINCT ev.id_message) AS visit_msg  -- Кількість переглянутих листів
  FROM `data-analytics-mate.DA.email_sent` es
  LEFT JOIN `DA.email_open` eo ON es.id_message = eo.id_message
  LEFT JOIN `DA.email_visit` ev ON es.id_message = ev.id_message
  JOIN `data-analytics-mate.DA.account` a ON es.id_account = a.id
  JOIN `DA.account_session` acs ON es.id_account = acs.account_id
  JOIN `DA.session_params` sp ON acs.ga_session_id = sp.ga_session_id
  JOIN `DA.session` s ON acs.ga_session_id = s.ga_session_id
  GROUP BY date, sp.country, send_interval, is_verified, is_unsubscribed
),




account_stats AS (
  -- Статистика по акаунтах (загальна кількість, верифікація, відписка)
  SELECT
    s.date AS date,  -- Дата сесії
    sp.country AS country,  -- Країна користувача
    a.send_interval AS send_interval,  -- Інтервал відправки для акаунта
    a.is_verified AS is_verified,  -- Перевірено акаунт чи ні
    a.is_unsubscribed AS is_unsubscribed,  -- Відписаний акаунт чи ні
    COUNT(DISTINCT acs.account_id) AS account_cnt,  -- Кількість унікальних акаунтів
    0 AS sent_msg,
    0 AS open_msg,
    0 AS visit_msg
  FROM `data-analytics-mate.DA.account` a
  JOIN `DA.account_session` acs ON a.id = acs.account_id
  JOIN `DA.session` s ON acs.ga_session_id = s.ga_session_id
  JOIN `DA.session_params` sp ON s.ga_session_id = sp.ga_session_id
  GROUP BY date, sp.country, a.send_interval, a.is_verified, a.is_unsubscribed
),


combined_stats AS (
  -- Об'єднуємо дані по акаунтах та електронним листам
  SELECT
    date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed,
    account_cnt,
    sent_msg,
    open_msg,
    visit_msg
  FROM account_stats


  UNION ALL


  SELECT
    date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed,
    account_cnt,
    sent_msg,
    open_msg,
    visit_msg
  FROM email_stats
),


aggregated_stats AS (
  -- Агрегуємо об'єднані дані по групах
  SELECT
    date,
    country,
    send_interval,
    is_verified,
    is_unsubscribed,
    SUM(account_cnt) AS account_cnt,
    SUM(sent_msg) AS sent_msg,
    SUM(open_msg) AS open_msg,
    SUM(visit_msg) AS visit_msg
  FROM combined_stats
  GROUP BY date, country, send_interval, is_verified, is_unsubscribed
),


country_totals AS (
  -- Обчислюємо загальну кількість акаунтів та листів по кожній країні
  SELECT
    *,
    SUM(account_cnt) OVER (PARTITION BY country) AS total_country_account_cnt,
    SUM(sent_msg) OVER (PARTITION BY country) AS total_country_sent_cnt
  FROM aggregated_stats
),


ranked_data AS (
  -- Визначаємо рейтинг країн за кількістю акаунтів та листів
  SELECT
    *,
    DENSE_RANK() OVER (ORDER BY total_country_account_cnt DESC) AS rank_total_country_account_cnt,
    DENSE_RANK() OVER (ORDER BY total_country_sent_cnt DESC) AS rank_total_country_sent_cnt
  FROM country_totals
)


-- Остаточний вибір даних для відображення топ-10 країн
SELECT
  date,
  country,
  send_interval,
  is_verified,
  is_unsubscribed,
  account_cnt,
  sent_msg,
  open_msg,
  visit_msg,
  total_country_account_cnt,
  total_country_sent_cnt,
  rank_total_country_account_cnt,
  rank_total_country_sent_cnt
FROM ranked_data
WHERE rank_total_country_account_cnt <= 10 OR rank_total_country_sent_cnt <= 10
ORDER BY country, date;
