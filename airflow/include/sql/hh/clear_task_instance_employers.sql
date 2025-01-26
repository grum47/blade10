delete 
from 	task_instance ti 
where 	dag_id = '01_07_stage_get_employers'
and 	task_id Like '%transform_data_employers%';