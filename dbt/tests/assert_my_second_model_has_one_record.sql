select 'error!' as error
where (select count(*) from {{ref('my_second_model')}}) != 1