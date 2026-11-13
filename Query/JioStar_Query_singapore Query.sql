WITH final_report AS (
    SELECT
        *,
        CASE 
			WHEN sku_name ='in.startv.hotstar.premium.ca.quarterly.self' THEN 49933
			WHEN sku_name ='HSPremium.CA.Year.4999' THEN 49934
			WHEN sku_name ='HSPremium.CA.Year.4999.NEW' THEN 49934
		END as material_code,
        CASE payment_gateway
            WHEN 'ADYEN' THEN 50002612
        END AS gateway_name,
        CASE
            WHEN transaction_type = 'REFUND'
                THEN -ABS(ABS(payment_amount::numeric) - tax_amount::numeric)
            ELSE hs_balance::numeric
        END AS final_amount
    FROM jiostar_data.december_internal_sg_data
),	
aggregated_report AS (
    SELECT
        gateway_name,
        material_code,
        service_start_date,
        service_end_date::DATE AS serviceenddate,
        state,
        COUNT(*) AS order_quantity,
        SUM(final_amount) AS rate_amount
    FROM final_report
    GROUP BY
        gateway_name,
        material_code,
        service_start_date,
        service_end_date::DATE,
        state
  
)


SELECT
    '' AS "UNIQUE IDENTIFIER",
    'ZSVD' AS "CONTRACT TYPE",
    1100 AS "SALES ORG",
    13 AS "DISTRIBUTION CHANNEL",
    '' AS "DIVISION",
    '' AS "SALES OFFICE",
    gateway_name AS "SOLD TO PARTY",
    '' AS "SHIP TO PARTY",
    '' AS "PO NUMBER",
    '' AS "PO DATE",
    'I001' AS "SALES REGION",
    '' AS "DESCRIPTION",
    '' AS "CONTRACT START DATE",
    '' AS "CONTRACT END DATE",
    '' AS "ORDER REASON",
	ROW_NUMBER() OVER (ORDER BY order_quantity DESC) AS "ITEM",
    material_code AS "MATERIAL CODE",
    '' AS "MATERIAL DESCRIPTION",
    order_quantity AS "ORDER QUANTITY",
    '' AS "UNIT OF MEASUREMENT",
    service_start_date AS "BILLING PLAN START DATE",
    serviceenddate AS "BILLING PLAN END DATE",
    '' AS "PROFIT CENTRE",
    '' AS "CONDITION TYPE",
    rate_amount AS "RATE AMOUNT",
    'CAD' AS "CURRENCY",
    '' AS "CONDITION VALUE",
    '' AS "RULE FOR ORIGIN",
    '' AS "RIGHTS",
    '' AS "COUNTRY",
    state AS "State"
FROM aggregated_report
ORDER BY gateway_name ASC, service_start_date::DATE ASC, serviceenddate::DATE ASC;



SELECT * 
FROM jiostar_data.december_internal_sg_data;


