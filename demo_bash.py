from __future__ import annotations
from functools import cached_property
from datetime import datetime

from airflow import models
from airflow.operators.bash import BashOperator
from airflow.hooks.subprocess import SubprocessHook


class MySubprocessHook(SubprocessHook):
    def __init__(self) -> None:
        self.sub_process: Popen[bytes] | None = None
        super().__init__()

class MyBashOperator(BashOperator):
    def __init__(
            self,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)

    def execute(self, context: Context):
        r=super(MyBashOperator, self).execute(context)
        print("hello " + r)
        return r

    @cached_property
    def subprocess_hook(self):
        """Returns hook for running the bash command."""
        return MySubprocessHook()


with models.DAG(
        dag_id="demo-bash",
        schedule="@once",  # Override to match your needs
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=["trino"],
) as dag:

    step1 = MyBashOperator(
        task_id="step1",
        bash_command="echo {{execution_date}} && echo 1234"
    )