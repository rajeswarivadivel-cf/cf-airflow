with chbk as (
		select 
			chbk.EXTERNAL_REF as CharegebackRef,
			sale.DESCRIPTION,
			CAST(COALESCE(CAST(sale.AMOUNT_MINOR_UNITS / POWER(10, 2)  AS DECIMAL(18, 2)), 0) AS NVARCHAR(100)) as SaleAmount,
			format(sale.TRANSACTION_TIME,'yyyy-MM-dd hh:mm:ss') as SaleDateTime,			
			chbk.DESCRIPTION as Chargeback,	
			format(chbk.TRANSACTION_TIME,'yyyy-MM-dd hh:mm:ss') as ChargebackTime,					
			rpr.DESCRIPTION as Representment,			
			format(rpr.TRANSACTION_TIME,'yyyy-MM-dd hh:mm:ss') as RepresentmentTime,
			secndchbk.DESCRIPTION as SecondChargeback,			
			format(secndchbk.TRANSACTION_TIME,'yyyy-MM-dd hh:mm:ss') as SecondChargeBackTime,
			chbkrvr.DESCRIPTION as Chargebackreversal,			
			format(chbkrvr.TRANSACTION_TIME,'yyyy-MM-dd hh:mm:ss') as ChargebackReversalTime,

			datediff(DAY,rpr.TRANSACTION_TIME,secndchbk.TRANSACTION_TIME) as RepresentmentToSecondChargeDays,
			datediff(DAY,secndchbk.TRANSACTION_TIME,coalesce(chbkrvr.TRANSACTION_TIME,secndchbk.TRANSACTION_TIME)) as SecondChargeToReversalDays,
			datediff(DAY,secndchbk.TRANSACTION_TIME,CURRENT_TIMESTAMP) as ReversalWaitPeriod,
			datediff(DAY,rpr.TRANSACTION_TIME,CURRENT_TIMESTAMP) as RepresentationWaitPeriod,
			datediff(DAY,Chbk.TRANSACTION_TIME,CURRENT_TIMESTAMP) as ChargeBackWaitPeriod,
			td.CUST_EMAIL
			FROM 
			accounts.dbo.acc_transactions sale 
			LEFT JOIN accounts.dbo.acc_transactions chbk ON sale.TRANSACTION_ID = chbk.TXN_FIRST_ID and chbk.EVENT_TYPE_ID = 11
			LEFT JOIN accounts.dbo.acc_transactions chbkrvr ON sale.TRANSACTION_ID = chbkrvr.TXN_FIRST_ID and chbk.EVENT_TYPE_ID = 12
			LEFT JOIN accounts.dbo.acc_transactions rpr ON sale.TRANSACTION_ID = rpr.TXN_FIRST_ID and rpr.EVENT_TYPE_ID = 13
			LEFT JOIN accounts.dbo.acc_transactions secndchbk ON sale.TRANSACTION_ID = secndchbk.TXN_FIRST_ID and secndchbk.EVENT_TYPE_ID = 61
			left join core..TRANS_DETAILS td on td.TRAN_REF = sale.EXTERNAL_REF

			WHERE 
			sale.SOURCE_REF = '{merchant_id}'
			and sale.EVENT_TYPE_ID = 0
			and sale.TRANSACTION_TIME >= '{start_date}'
			and chbk.DESCRIPTION is not null
		)
		select 
		 chbk.CharegebackRef,
		 chbk.DESCRIPTION,
		 chbk.SaleAmount,chbk.SaleDateTime,
		 chbk.Chargeback,chbk.ChargebackTime,
		 chbk.Representment,chbk.RepresentmentTime,
		 chbk.SecondChargeback,chbk.SecondChargeBackTime,
		 chbk.Chargebackreversal,chbk.ChargebackReversalTime,
		 chbk.RepresentmentToSecondChargeDays,chbk.SecondChargeToReversalDays,chbk.RepresentationWaitPeriod,chbk.ReversalWaitPeriod,chbk.CUST_EMAIL,
			case when (RepresentationWaitPeriod > 45 
					   and  RepresentmentToSecondChargeDays is null
					   )
					   OR (RepresentmentToSecondChargeDays between 1 and 45 
					   and SecondChargeToReversalDays between 1 and 30					   
					   )
				  then 'Won'
				  when (RepresentmentToSecondChargeDays between 1 and 45 
					   and SecondChargeToReversalDays = 0 
					   and ReversalWaitPeriod > 30  
					   )
					   or 
					   (Representment is null and ChargeBackWaitPeriod > 45) 
					   or 
					   (RepresentmentToSecondChargeDays > 45 
					   and SecondChargeToReversalDays = 0 
					   and ReversalWaitPeriod > 30 
					   )
				  then 'Lost'
				  when (RepresentmentToSecondChargeDays between 1 and 45 
					   and SecondChargeToReversalDays = 0 
					   and ReversalWaitPeriod < 30 					   
					   )
					OR 
					  (RepresentationWaitPeriod <= 45 
					   and  RepresentmentToSecondChargeDays is null 
					   )					
					OR (Representment is null and ChargeBackWaitPeriod <= 45) 
				  then 'Pending'


			end as Status
		from  chbk
		order by chbk.SaleDateTime