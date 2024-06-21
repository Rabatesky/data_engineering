import subprocess
import shutil
import os
from datetime import datetime

def update_dags_and_scripts(repo_url, local_path, airflow_path, proxy_url):
    try:
        env = {'HTTP_PROXY': proxy_url, 'HTTPS_PROXY': proxy_url}

        # Проверяем, существует ли локальная папка репозитория
        if not os.path.exists(local_path):
            print("Local path does not exist. Cloning the repository...")
            subprocess.check_call(['git', 'clone', repo_url, local_path])

        # Переключаемся в папку репозитория
        os.chdir(local_path)

        # Получаем последние изменения из репозитория
        subprocess.check_call(['git', 'pull'], env=env)

        # Перескопировать содержимое папок dags и scripts
        dags_src = os.path.join(local_path, 'dags')
        scripts_src = os.path.join(local_path, 'scripts')
        dags_dest = os.path.join(airflow_path, 'dags')  # Измените на актуальный путь
        scripts_dest = os.path.join(airflow_path, 'scripts')  # Измените на актуальный путь

        shutil.rmtree(dags_dest, ignore_errors=True)
        shutil.rmtree(scripts_dest, ignore_errors=True)

        shutil.copytree(dags_src, dags_dest)
        shutil.copytree(scripts_src, scripts_dest)

    except Exception as e:
        print(f'Error during update: {e}')

if __name__ == '__main__':
    repo_url = 'https://github.com/Rabatesky/data_engineering.git' #ссылка на git
    local_path = '/git_rep/data_engineering'  # Путь к локальному репозиторию на сервере
    airflow_path = '/airflow' #папка где лежит airflow(AIRFLOW_HOME)
    proxy_url = "http://10.155.110.30:3128/"
    update_dags_and_scripts(repo_url, local_path, airflow_path, proxy_url)
