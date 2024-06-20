default_args = {
    'owner': 'home_pc',                             #Владелец дага
    'queue': 'home_pc_queue',                       #Очередь в которую становится task
    'pool': 'user_pool',                            #пул в рамках которого исполняется такст
    'email': ['...'],                               #Нуже для отправки писем сигнализирующих о падении или перезапуске дага
    'email_on_failure': False,                      #Отправка ошибок
    'email_on_retry': False,                        #Отправка перезапусков
    'depends_on_past': False,                       #Таск в данном dag инстансе будет запущен только в тот момент, когда этот же task в предыдущем dag инстансе уже был отработан
    'wait_for_downstream': False,                   #Ждать окончания всех таском которые зависят от этого(в прошлом запуске дага)
    'retries': 3,                                   #Количество перезапусков в случае падения
    'retry_delay': timedelta(minutes=5),            #Время через которое стоит выполнить перезапуск
    'priority_weight': 10,                          #
    'start_date': datetime(2021, 1, 1),             #Дата первого запуска, отработает столько раз сколько прошло времени с первого запуска
    'catchup': False,                               #Если false  то будет запущен только 1 раз в момент запуска(игнор start_date)
    'end_date': datetime(2025, 1, 1),               #После этого времени таск перестанет запускаться
    'sla': timedelta(hours=2),                      #Время за которое поидее даг уже должен закончить работу, если нет то придёт уведомление что таск работал дольше
    'execution_timeout': timedelta(seconds=300),    #Максимально время на таск, после этого break
    'on_failure_callback': some_function,           #Функция будет вызвана в случае падения таска
    'on_success_callback': some_other_function,     #в случае успеха
    'on_retry_callback': another_function,          #в случае перезапуска
    'sla_miss_callback': yet_another_function,      #в случае окончания sla
    'trigger_rule': 'all_success'                   #Случаи запуска таска(all_success,all_failed,all_done,one_failed,one_success,none_failed,none_failed_or_skipped,none_skipped,dummy)
}