WITH final_report AS (
    SELECT
        *,
        CASE 
			WHEN skuname ='Bonanza.Mobile.IN.Monthly.30Days' THEN 49901
			WHEN skuname ='HotstarMobile.IN.1Month.19' THEN 49902
			WHEN skuname ='HotstarMobile.IN.1Month.29' THEN 49903
			WHEN skuname ='Bonanza.Mobile.IN.Monthly.30Days.49' THEN 49904
			WHEN skuname ='HotstarMobile.IN.1Month.49' THEN 49904
			WHEN skuname ='HotstarMobile.IN.1Month.79' THEN 49905
			WHEN skuname ='HotstarMobile.IN.1Month.99' THEN 49906
			WHEN skuname ='HotstarMobile.IN.1Month.129' THEN 49907
			WHEN skuname ='Bonanza.Premium.IN.Yearly.30Days' THEN 49908
			WHEN skuname ='BBPS.IN.Premium.Annual' THEN 49909
			WHEN skuname ='HotstarPremium.IN.Year.1499' THEN 49909
			WHEN skuname ='HotstarPremium.IN.Year.1799' THEN 49910
			WHEN skuname ='HotstarPremium.IN.Year.1999' THEN 49911
			WHEN skuname ='HotstarPremium.IN.Year.2199' THEN 49912
			WHEN skuname ='in.startv.hotstar.premium.annual.android' THEN 49912
			WHEN skuname ='Bonanza.Mobile.IN.Yearly.30Days' THEN 49913
			WHEN skuname ='Bonanza.Super.IN.Yearly.30Days' THEN 49914
			WHEN skuname ='HotstarMobile.IN.Year.199' THEN 49915
			WHEN skuname ='HotstarMobile.IN.Year.299' THEN 49916
			WHEN skuname ='BBPS.IN.Mobile.Annual' THEN 49917
			WHEN skuname ='HotstarMobile.IN.Year.499' THEN 49917
			WHEN skuname ='in.startv.hotstar.mobile.annual.android' THEN 49917
			WHEN skuname ='HotstarMobile.IN.Year.599' THEN 49918
			WHEN skuname ='HotstarMobile.AdsFree.IN.Year.699' THEN 49919
			WHEN skuname ='in.startv.hotstar.mobile.annual.adsfree' THEN 49919
			WHEN skuname ='BBPS.IN.Super.Annual' THEN 49920
			WHEN skuname ='HotstarSuper.IN.Year.899' THEN 49920
			WHEN skuname ='HotstarSuper.IN.Year.999' THEN 49921
			WHEN skuname ='HotstarSuper.AdsFree.IN.Year.1099' THEN 49922
			WHEN skuname ='HotstarSuper.IN.Year.1099' THEN 49922
			WHEN skuname ='in.startv.hotstar.super.annual.adsfree' THEN 49922
			WHEN skuname ='in.startv.hotstar.super.annual.android' THEN 49922
			WHEN skuname ='HotstarSuper.IN.Year.1199' THEN 49923
			WHEN skuname ='HotstarPremium.IN.Year.2499' THEN 49924
			WHEN skuname ='HotstarMobile.IN.6Month.299' THEN 49925
			WHEN skuname ='Bonanza.Premium.IN.Monthly.30Days' THEN 49926
			WHEN skuname ='Bonanza.Super.IN.Monthly.30Days' THEN 49927
			WHEN skuname ='Bonanza.Super.IN.Monthly.30Days.99' THEN 49928
			WHEN skuname ='HotstarSuper.IN.1Month.99' THEN 49928
			WHEN skuname ='HotstarSuper.IN.1Month.149' THEN 49929
			WHEN skuname ='HotstarSuper.IN.1Month.179' THEN 49930
			WHEN skuname ='Bonanza.Premium.IN.Monthly.30Days.199' THEN 49931
			WHEN skuname ='HotstarPremium.IN.Month.199' THEN 49931
			WHEN skuname ='HotstarSuper.IN.1Month.199' THEN 49932
			WHEN skuname ='HotstarPremium.IN.Month.249' THEN 49933
			WHEN skuname ='HotstarPremium.IN.Month.299' THEN 49934
			WHEN skuname ='HotstarPremium.IN.Month.349' THEN 49935
			WHEN skuname ='Bonanza.Mobile.IN.Quarterly.30Days' THEN 49936
			WHEN skuname ='Bonanza.Premium.IN.Quarterly.30Days' THEN 49937
			WHEN skuname ='Bonanza.Super.IN.Quarterly.30Days' THEN 49938
			WHEN skuname ='Bonanza.Mobile.IN.Quarterly.30Days.129' THEN 49939
			WHEN skuname ='HotstarMobile.IN.3Month.129' THEN 49939
			WHEN skuname ='BBPS.IN.Mobile.Quarterly' THEN 49940
			WHEN skuname ='HotstarMobile.IN.3Month.149' THEN 49940
			WHEN skuname ='in.startv.hotstar.mobile.3month.android' THEN 49940
			WHEN skuname ='HotstarMobile.IN.3Month.199' THEN 49941
			WHEN skuname ='Bonanza.Super.IN.Quarterly.30Days.249' THEN 49942
			WHEN skuname ='HotstarSuper.IN.3Month.249' THEN 49942
			WHEN skuname ='BBPS.IN.Super.Quarterly' THEN 49943
			WHEN skuname ='HotstarSuper.IN.3Month.299' THEN 49943
			WHEN skuname ='in.startv.hotstar.super.3mon.android.299' THEN 49943
			WHEN skuname ='HotstarSuper.IN.3Month.329' THEN 49944
			WHEN skuname ='HotstarSuper.IN.3Month.349' THEN 49945
			WHEN skuname ='HotstarSuper.IN.3Month.399' THEN 49946
			WHEN skuname ='BBPS.IN.Premium.Quarterly' THEN 49947
			WHEN skuname ='HotstarPremium.IN.3Month.499' THEN 49947
			WHEN skuname ='in.startv.hotstar.prem.3mon.android.499' THEN 49947
			WHEN skuname ='HotstarPremium.IN.3Month.549' THEN 49948
			WHEN skuname ='HotstarPremium.IN.3Month.599' THEN 49949
			WHEN skuname ='HotstarPremium.IN.3Month.699' THEN 49950
			WHEN skuname ='HotstarPremium.IN.3Month.799' THEN 49951
			WHEN skuname ='HotstarMobile.AdsFree.IN.3Month.249' THEN 49952

		END as material_code,
        CASE paymentgateway
            WHEN 'PAYU' THEN 10001675
            WHEN 'PHONEPE' THEN 10003620
            WHEN 'RAZORPAY' THEN 50001395
			WHEN 'RAZOR' THEN 50001395
            WHEN 'GOOGLE' THEN 10003088
            WHEN 'PAYTM' THEN 10000673
            WHEN 'BBPS' THEN 10006171
            WHEN 'BILLDESK' THEN 50002188
        END AS gateway_name,
        CASE
            WHEN transactiontype = 'REFUND'
                THEN -ABS(ABS(paymentamount::numeric) - taxamount::numeric)
            ELSE hsbalance
        END AS final_amount
    FROM cointab.ir_files
),	
aggregated_report AS (
    SELECT
        gateway_name,
        material_code,
        servicestartdate,
        serviceenddate::DATE AS serviceenddate,
        state,
        COUNT(*) AS order_quantity,
        SUM(final_amount) AS rate_amount
    FROM final_report
    GROUP BY
        gateway_name,
        material_code,
        servicestartdate,
        serviceenddate::DATE,
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
    servicestartdate AS "BILLING PLAN START DATE",
    serviceenddate AS "BILLING PLAN END DATE",
    '' AS "PROFIT CENTRE",
    '' AS "CONDITION TYPE",
    rate_amount AS "RATE AMOUNT",
    'INR' AS "CURRENCY",
    '' AS "CONDITION VALUE",
    '' AS "RULE FOR ORIGIN",
    '' AS "RIGHTS",
    '' AS "COUNTRY",
    state AS "State"
FROM aggregated_report
ORDER BY gateway_name ASC, servicestartdate::DATE ASC, serviceenddate::DATE ASC



