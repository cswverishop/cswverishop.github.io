---
layout: default
lang: zh
lang_title: SQL样式指南
contributors:
    - user: wontoncoder
      type: translator
    - user: penghou620
      type: correction
---

* TOC
{:toc}
CREATE OR REPLACE VIEW LOOKER.CREATOR_LINKING_ORDER_ATTRIBUTION
AS (
WITH ORDER_FACTS AS (
     SELECT
        me.looker_visitor_id
        , me.order_id
        , me.received_at
        , o.customer_id
        , ROW_NUMBER() OVER (PARTITION BY me.order_id ORDER BY me.received_at DESC) AS rn
    FROM segment_events.verishop_prod.pre_mapped_events me
    LEFT JOIN (
        SELECT
          DISTINCT
            id AS order_id,
            customer_id
        FROM
           looker.orders
      WHERE ORDER_ID = 4099489628354
    ) o ON o.order_id = me.order_id
    WHERE me.event = 'order_completed'
      AND me.order_id IS NOT NULL
      AND me.received_at>dateadd(day,-7,current_date) --period to look back for insert new records
)

SELECT
          DISTINCT
            e.order_id
          , e.looker_visitor_id
          , e.customer_id
          , e.order_received_at
          , first_value(e.query_string IGNORE NULLS)
              over (partition by e.order_id order by e.received_at rows between unbounded preceding and unbounded following) as first_query_string
          , regexp_substr(first_query_string,'utm_source=(.*)&',1, 1, 'e') as first_creator_linking_source
          , regexp_substr(first_query_string,'source_creator_id=(.*)',1, 1, 'e') as first_creator_linking_creator_id
          , last_value(e.query_string IGNORE NULLS)
              over (partition by e.order_id order by e.received_at rows between unbounded preceding and unbounded following) as last_query_string
          , regexp_substr(last_query_string,'utm_source=(.*)&',1, 1, 'e') as last_creator_linking_source
          , regexp_substr(last_query_string,'source_creator_id=(.*)',1, 1, 'e') as last_creator_linking_creator_id
        FROM
        (
          SELECT
          o.looker_visitor_id 
          , o.order_id
          , o.customer_id
          , o.received_at AS order_received_at
          , e.received_at
          , e.event_id
          , e.event
          , e.referrer
          , e.query_string
          , e.url
        FROM ORDER_FACTS o
        JOIN segment_events.verishop_prod.pre_mapped_events e
           ON o.looker_visitor_id = e.looker_visitor_id
            AND e.received_at <= o.received_at
            AND e.received_at >= DATEADD('day', -7, o.received_at) --change to 7 days lookback period 
        WHERE  o.rn = 1 
            AND REGEXP_INSTR(LOWER(e.query_string), '.*(utm_source=creator|source_creator_id).*') > 0
        )  e
);
{% include sqlstyle.guide.zh.md %}
