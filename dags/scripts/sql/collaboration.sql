with dispute_collaboration as (
		select 
			dispute.EXTERNAL_REF as DisputeRef,
			sale.DESCRIPTION,
			CAST(COALESCE(CAST(sale.AMOUNT_MINOR_UNITS / POWER(10, 2)  AS DECIMAL(18, 2)), 0) AS NVARCHAR(100)) as SaleAmount,
			format(sale.TRANSACTION_TIME,'yyyy-MM-dd hh:mm:ss') as SaleDateTime,			
			dispute.DESCRIPTION as Dispute,			
			format(dispute.TRANSACTION_TIME,'yyyy-MM-dd hh:mm:ss') as Dispute_time,
			disputeresp.DESCRIPTION as DisputeResponse,			
			format(disputeresp.TRANSACTION_TIME,'yyyy-MM-dd hh:mm:ss') as DisputeResponseTime,
			disputerever.DESCRIPTION as Dispute_Reversal,			
			format(disputerever.TRANSACTION_TIME,'yyyy-MM-dd hh:mm:ss') as DisputeReversalTime,
			disputeprearb.DESCRIPTION as Dispute_Pre_Arb,			
			format(disputeprearb.TRANSACTION_TIME,'yyyy-MM-dd hh:mm:ss') as DisputePreArbTime,
			datediff(DAY,disputeresp.TRANSACTION_TIME,disputeprearb.TRANSACTION_TIME) as DisputeResponseToPreArbPeriod,
			datediff(DAY,disputeresp.TRANSACTION_TIME,disputerever.TRANSACTION_TIME) as DisputeResponseToReversalPeriod,
			datediff(DAY,disputeprearb.TRANSACTION_TIME,disputerever.TRANSACTION_TIME) as DisputePreArbToReversalPeriod,
			--datediff(DAY,dispute.TRANSACTION_TIME,disputerever.TRANSACTION_TIME) as DisputeToReversalPeriod,
			datediff(DAY,disputeresp.TRANSACTION_TIME,CURRENT_TIMESTAMP) as ResponseWaitPeriod,			
			datediff(DAY,disputeprearb.TRANSACTION_TIME,CURRENT_TIMESTAMP) as PreArbToReversalWaitPeriod,
			datediff(DAY,dispute.TRANSACTION_TIME,CURRENT_TIMESTAMP) as DisputeWaitPeriod,
			td.CUST_EMAIL
			FROM 
			accounts.dbo.acc_transactions sale 
			LEFT JOIN accounts.dbo.acc_transactions dispute ON sale.TRANSACTION_ID = dispute.TXN_FIRST_ID and dispute.EVENT_TYPE_ID = 71
			LEFT JOIN accounts.dbo.acc_transactions disputeresp ON sale.TRANSACTION_ID = disputeresp.TXN_FIRST_ID and disputeresp.EVENT_TYPE_ID = 73
			LEFT JOIN accounts.dbo.acc_transactions disputeprearb ON sale.TRANSACTION_ID = disputeprearb.TXN_FIRST_ID and disputeprearb.EVENT_TYPE_ID = 74
			LEFT JOIN accounts.dbo.acc_transactions disputerever ON sale.TRANSACTION_ID = disputerever.TXN_FIRST_ID and disputerever.EVENT_TYPE_ID = 77
			--LEFT JOIN accounts.dbo.acc_transactions disputeliab ON sale.TRANSACTION_ID = disputeliab.TXN_FIRST_ID and disputeliab.EVENT_TYPE_ID = 78
			LEFT JOIN accounts.dbo.acc_transactions disputearb ON sale.TRANSACTION_ID = disputearb.TXN_FIRST_ID and disputearb.EVENT_TYPE_ID = 79

			LEFT JOIN accounts.dbo.acc_transactions hrw ON sale.TRANSACTION_ID = hrw.TXN_FIRST_ID and hrw.EVENT_TYPE_ID = 8
			left join core.dbo.sales s with (nolock) on s.REFERENCE = sale.EXTERNAL_REF
			left JOIN core.dbo.SALE_AUTH_RESPONSES sar with (nolock) ON s.id=sar.ID
			left JOIN core.dbo.SALEDISPUTES sd with (nolock) ON sar.ACQ_REF_NUM=sd.ORIG_SALE_ARN
			left JOIN [core].[dbo].[CLEARING_REASON_CODES] rc with (nolock)	 ON rc.REASON_CODE = sd.REASON AND rc.TRAN_TYPE = 45
			LEFT JOIN [core].[dbo].[CLEARING_REASON_SUB_CODES] rcs with (nolock)  ON rcs.REASON_SUB_CODE = sd.dispute_condition  AND rcs.REASON_CODE = sd.REASON  AND rcs.TRAN_TYPE = 45
			left join core..TRANS_DETAILS td on td.TRAN_REF = s.REFERENCE

			WHERE 
			sale.SOURCE_REF = '{merchant_id}'
			and sale.EVENT_TYPE_ID = 0
			and sale.TRANSACTION_TIME >= '{start_date}'			
			and dispute.DESCRIPTION  is not null 			
			and not rc.DESCRIPTION like '%fraud%'
		)
		select *,
		case when (ResponseWaitPeriod > 30 and DisputeResponseToPreArbPeriod is null and DisputeResponseToReversalPeriod is null)  
				  OR Dispute_Reversal is not null 
			  then 'Won' 
			 when (DisputeResponseToPreArbPeriod is not null and DisputePreArbToReversalPeriod is null and PreArbToReversalWaitPeriod > 30) 
				  OR 
				  (DisputeWaitPeriod > 30 and DisputeResponse is null and Dispute_Reversal is null and Dispute_Pre_Arb is null) 
			  then 'Lost' 
			 when (DisputeResponseToPreArbPeriod is not null and PreArbToReversalWaitPeriod <= 30 )		 
				  OR (DisputeWaitPeriod <= 30 and DisputeResponse is null and Dispute_Reversal is null and Dispute_Pre_Arb is null)
				  OR (ResponseWaitPeriod <= 30 and Dispute_Reversal is null and Dispute_Pre_Arb is null)
			 then 'Pending' 
		end as Status
		from dispute_collaboration
		order by SaleDateTime 