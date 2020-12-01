from dataclasses import dataclass
from typing import Tuple


@dataclass(eq=True, frozen=True)
class Task:
    """Class to store details of each task
    regardless of type
    """

    job_id: str
    task_id: str
    duration: int


@dataclass
class WorkerMessage:
    """Class to store messages from worker"""

    addr: Tuple[str, int]
    message: str
    task_id: int
    job_id: int
    w_id: int
