SELECT year, month, day, COUNT(*) AS pageviews
FROM wmf.webrequest
WHERE year = 2015
AND month = 04
AND webrequest_source IN('text','mobile')
AND is_pageview = true
GROUP BY year, month, day;