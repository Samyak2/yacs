from dataclasses import dataclass


@dataclass
class Task:
    """Class to store details of each task
    regardless of type
    """

    task_id: str
    duration: int
