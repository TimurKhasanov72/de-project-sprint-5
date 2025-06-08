-- cdm.dm_courier_ledger определение

-- Drop table

DROP TABLE cdm.dm_courier_ledger;

CREATE TABLE cdm.dm_courier_ledger (
	id serial4 NOT NULL,
	courier_id varchar NOT NULL,
	courier_name varchar NOT NULL,
	settlement_year int4 NOT NULL,
	settlement_month int4 NOT NULL,
	orders_count int4 NULL,
	orders_total_sum numeric(14, 2) null,
	rate_avg numeric(14, 2) NULL,
	order_processing_fee numeric(14, 2) NULL,
	courier_order_sum numeric(14, 2) NULL,
	courier_tips_sum numeric(14, 2) NULL,
	courier_reward_sum numeric(14, 2) NULL,
	CONSTRAINT dm_courier_ledger_pkey PRIMARY KEY (id),
	CONSTRAINT dm_courier_ledger_settlement_month_check CHECK (((1 <= settlement_month) AND (settlement_month <= 12))),
	CONSTRAINT dm_courier_ledger_settlement_year_check CHECK (((2025 <= settlement_year) AND (settlement_year < 2100)))
);

with couriers_orders as (
    select distinct
        de.courier_id,
        co.courier_name,
        dt.year as settlement_year,
        dt.month as settlement_month,
        do2.id as order_id,
        fps.total_sum,
        de.tip_sum,
        de.rate        
    from dds.dm_orders do2 
    join dds.dm_timestamps dt on do2.timestamp_id = dt.id
    join dds.dm_deliveries de on do2.id = de.order_id
    join dds.dm_couriers co on de.courier_id = co.id
    join dds.fct_product_sales fps on fps.order_id = do2.id
    where do2.order_status = 'CLOSED'
),
couriers_stat as (
	select 	
		co.courier_id,
		co.courier_name,
		co.settlement_year,
		co.settlement_month,
		count(order_id) as orders_count,
		sum(total_sum) as orders_total_sum,
		avg(rate) as rate_avg,
		sum(tip_sum) as courier_tips_sum,
		sum(total_sum) * 0.25 as order_processing_fee
	from couriers_orders co
	group by courier_id, courier_name, settlement_year, settlement_month
	order by courier_name, settlement_year, settlement_month 
),
couriers_order_sum as (  -- сумма за доставку с учетом рейтинга
	select 
	    co.courier_id,
	    co.settlement_year,
	    co.settlement_month,
--	    co.total_sum,
--	    cs.rate_avg,
		case
			when cs.rate_avg < 4 then 
				case when co.total_sum * 0.05 < 100 then 100
					else co.total_sum * 0.05
				end		
			when 4 <= cs.rate_avg and cs.rate_avg < 4.5 then 
				case when co.total_sum * 0.07 < 150 then 150
					else co.total_sum * 0.07
				end
			when 4.5 <= cs.rate_avg and cs.rate_avg < 4.9 then 
				case when co.total_sum * 0.08 < 175 then 175
					else co.total_sum * 0.08
				end
			when 4.9 <= cs.rate_avg then 
				case when co.total_sum * 0.1 < 200 then 200
					else co.total_sum * 0.1
				end
		end courier_order_sum
	from couriers_stat cs
	join couriers_orders co 
		on cs.courier_id = co.courier_id 
		and cs.settlement_year = co.settlement_year 
		and cs.settlement_month = co.settlement_month
),
couriers_order_sum_by_month as ( -- сумма за доставку с учетом рейтинга за месяц
	select 
	    courier_id,
	    settlement_year,
	    settlement_month,	
		sum(courier_order_sum) as courier_order_sum
	from couriers_order_sum 
	group by courier_id, settlement_year, settlement_month
)
insert into cdm.dm_courier_ledger (	
	courier_id,
	courier_name,
	settlement_year,
	settlement_month,
	orders_count,
	orders_total_sum,
	rate_avg,
	order_processing_fee,
	courier_order_sum,
	courier_tips_sum,
	courier_reward_sum
)
select 
	cs.courier_id,
	cs.courier_name,
	cs.settlement_year,
	cs.settlement_month,
	cs.orders_count,
	cs.orders_total_sum,
	cs.rate_avg,
	cs.order_processing_fee,
	cos.courier_order_sum,
	cs.courier_tips_sum,
	cos.courier_order_sum + cs.courier_tips_sum * 0.95 as courier_reward_sum
from couriers_stat cs
join couriers_order_sum_by_month cos 
	on cs.courier_id = cos.courier_id 
	and cs.settlement_year = cos.settlement_year 
	and cs.settlement_month = cos.settlement_month;
