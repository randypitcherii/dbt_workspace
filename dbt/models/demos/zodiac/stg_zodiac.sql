-- Downloaded this seed set from https://www.kaggle.com/romanzdk/zodiacs-with-corresponding-dates

select
    *
from {{ ref('zodiac') }}