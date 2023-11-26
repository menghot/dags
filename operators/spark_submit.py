from __future__ import annotations

import contextlib
import os
import signal
from functools import cached_property
from subprocess import Popen, PIPE, STDOUT
from tempfile import gettempdir, TemporaryDirectory
from typing import Any

from airflow.operators.bash import BashOperator
from airflow.hooks.subprocess import SubprocessHook, SubprocessResult


class CustomSubprocessHook(SubprocessHook):

    def get_conn(self) -> Any:
        super().get_conn()

    def run_command(self, command: list[str], env: dict[str, str] | None = None, output_encoding: str = "utf-8",
                    cwd: str | None = None) -> SubprocessResult:

        self.log.info("Tmp dir root location: %s", gettempdir())

        with contextlib.ExitStack() as stack:
            if cwd is None:
                cwd = stack.enter_context(TemporaryDirectory(prefix="airflowtmp"))

            def pre_exec():
                # Restore default signal disposition and invoke setsid
                for sig in ("SIGPIPE", "SIGXFZ", "SIGXFSZ"):
                    if hasattr(signal, sig):
                        signal.signal(getattr(signal, sig), signal.SIG_DFL)
                os.setsid()

            self.log.info("Running command: %s", command)

            self.sub_process = Popen(
                command,
                stdout=PIPE,
                stderr=STDOUT,
                cwd=cwd,
                env=env if env or env == {} else os.environ,
                preexec_fn=pre_exec,
            )

            self.log.info("Output:")
            line = ""
            submit_status_result = ""
            if self.sub_process is None:
                raise RuntimeError("The subprocess should be created here and is None!")
            if self.sub_process.stdout is not None:
                for raw_line in iter(self.sub_process.stdout.readline, b""):
                    line = raw_line.decode(output_encoding, errors="backslashreplace").rstrip()

                    if "task" in line.lower():
                        submit_status_result = "spark job success"

                    self.log.info("# %s", line)

            self.sub_process.wait()

            self.log.info("Command exited with return code %s", self.sub_process.returncode)
            return_code: int = self.sub_process.returncode

        if submit_status_result:
            line = submit_status_result

        return SubprocessResult(exit_code=return_code, output=line)


class SparkBashSubmitOperator(BashOperator):

    @cached_property
    def subprocess_hook(self):
        """Returns hook for running the bash command."""
        return CustomSubprocessHook()
