with yodlee_transactions as (
    select
          acc.value:accountId::varchar                           as account_id,
          txt.value:amount::number(10,2)                         as amount,
          txt.value:baseType::varchar                            as base_type,
          txt.value:category::varchar                            as category,
          txt.value:categoryId::varchar                          as category_id,
          txt.value:categorySource::varchar                      as category_source,
          txt.value:categoryType::varchar                        as category_type,
          txt.value:container::varchar                           as container,
          nullif(txt.value:couponNumber::varchar, '')            as coupon_number,
          txt.value:createdDate::varchar                         as created_date,
          txt.value:description::varchar                         as description,
          txt.value:id::varchar                                  as transaction_id,
          txt.value:isManual::varchar                            as is_manual,
          txt.value:isPhysical::varchar                          as is_physical,
          txt.value:lastUpdated::varchar                         as last_updated,
          nullif(txt.value:memo::varchar, '')                    as memo,
          nullif(txt.value:merchant::varchar, '')                as merchant,
          txt.value:parentCategoryId::varchar                    as parentCategoryId,
          {{convert_date('txt.value:postDate::varchar')}}        as post_date,
          txt.value:runningBalance::number(10,2)                 as running_balance,
          nullif(txt.value:sourceId::varchar, '')                as source_id,
          txt.value:sourceType::varchar                          as source_type,
          txt.value:status::varchar                              as status,
          nullif(txt.value:subType::varchar, '')                 as sub_type,
          {{convert_date('txt.value:transactionDate::varchar')}} as transaction_date,
          txt.value:type::varchar                                as type
    from {{source('s3_raw_data','yodlee_transactions')}},
    lateral flatten(input=> variant_col:accounts) as acc,
    lateral flatten(input=> acc.value:transactions) as txt
)

select * from yodlee_transactions