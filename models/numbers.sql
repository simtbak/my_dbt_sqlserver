with n as (
	SELECT null x
	UNION ALL SELECT null
)

/*
SQL COMMENT 
{#
JINJA COMMENT 
JINJA COMMENT 
-#}
SQL COMMENT 
*/

SELECT row_number() OVER ( ORDER BY (SELECT NULL) ) n
FROM n p1
,n p2
,n p3
,n p4
,n p5