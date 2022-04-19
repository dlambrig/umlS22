from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from kubernetes.client import models as k8s
from airflow.kubernetes.secret import Secret


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'kubernetes_sample', default_args=default_args, schedule_interval=timedelta(minutes=10))


start = DummyOperator(task_id='run_this_first', dag=dag)

volume = k8s.V1Volume(
    name='test-volume',
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='centos-pv-claim'),
)

volume_mount = k8s.V1VolumeMount(
    name='test-volume', mount_path='/root/mount_file', sub_path=None
)

secret_pass = Secret(
    # Expose the secret as environment variable.
    deploy_type='env',
    # The name of the environment variable, since deploy_type is `env` rather
    # than `volume`.
    deploy_target='ACCESS',
    # Name of the Kubernetes Secret
    secret='aws-creds',
    # Key of a secret stored in this Secret object
    key='password')

secret_user = Secret(
    # Expose the secret as environment variable.
    deploy_type='env',
    # The name of the environment variable, since deploy_type is `env` rather
    # than `volume`.
    deploy_target='USER',
    # Name of the Kubernetes Secret
    secret='aws-creds',
    # Key of a secret stored in this Secret object
    key='username')

passing = KubernetesPodOperator(namespace='airflow',
                          image="dlambrig/python-uml:latest",
                          cmds=["bash","-c"],
                          arguments=["/run.sh"],
                          labels={"foo": "bar"},
                          name="passing-test",
                          volumes=[volume],
                          volume_mounts=[volume_mount],
                          task_id="passing-task",
                          get_logs=True,
                          secrets=[secret_pass, secret_user],
                          dag=dag
                          )

failing = KubernetesPodOperator(namespace='airflow',
                          image="ubuntu:latest",
                          cmds=["echo","made it here!"],
                          labels={"foo": "bar"},
                          name="fail",
                          task_id="failing-task",
                          get_logs=True,
                          dag=dag
                          )

passing.set_upstream(start)
failing.set_upstream(start)
