SELECT year, month, day, COUNT(*) AS pageviews
FROM wmf.webrequest
WHERE year = 2015
AND month = 04
AND geocoded_data['country_code'] != 'US'
GROUP BY year, month, day;