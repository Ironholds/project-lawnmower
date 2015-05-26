SELECT year, month, day, COUNT(*) AS pageviews
FROM wmf.webrequest
WHERE year = 2015
AND month = 04 AND day BETWEEN 21 AND 28
AND geocoded_data['country_code'] != 'US'
GROUP BY year, month, day;