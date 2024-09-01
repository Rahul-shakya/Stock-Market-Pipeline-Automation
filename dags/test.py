from airflow.decorators import dag, task
from datetime import datetime

@dag(start_date=datetime(2024, 8, 30),
     schedule='@daily',
     tags=['test']
)
def test():
    
    @task
    def t1():
        print('from t1')
        
    @task(wait_for_downstream = True)
    def t2():
        print('from t2')

    @task()
    def t3():
        # raise ValueError('this si drill.')
        print('from t3')
        
    @task
    def t4():
        print('from t4')

    t1() >> t2() >> t3() >> t4()
    
test()

