from argparse import ArgumentParser, Namespace
from queue import Queue, PriorityQueue

def read_jobs(input_file: str) -> list[Job]:
    with open(input_file, "r") as file:
        lines: list[list[str]] = [line.split(" ") for line in file.readlines()]
        jobs: list[Job] = []
        for index, job in enumerate(lines):
            jobs.append(Job(run_time=int(job[0]), arrival_time=int(job[1]), job_number=index))
        jobs.sort(key= lambda job: job.arrival_time)
        return jobs

class Job:
    def __init__(self, arrival_time: int, run_time: int, job_number: int):
        self.arrival_time: int = arrival_time
        self.run_time: int = run_time
        self.total_run_time: int = run_time
        self.job_number: int = job_number
        # set by scheduling algorithm
        self.first_run_time: int = 0
        self.completion_time: int = 0
        self.first_run: bool = True

    def get_first_run_time(self) -> int:
        return self.first_run_time
    
    def set_first_run_time(self, time: int) -> None:
        self.first_run_time = time

    def is_first_time_running(self) -> bool:
        return self.first_run
    
    def set_is_first_time_running(self, value: bool) -> None:
        self.first_run = value

    def get_completion_time(self) -> int:
        return self.completion_time
    
    def set_completion_time(self, time: int) -> None:
        self.completion_time = time

    def get_run_time(self) -> int:
        return self.run_time

    def set_run_time(self, time: int) -> None:
        self.run_time = time

    def get_arrival_time(self) -> int:
        return self.arrival_time
    
    def get_job_number(self) -> int:
        return self.job_number
    
    # turnaround time = completion time - arrival time (time between job arrival and job completion)
    def compute_turnaround_time(self) -> float:
        return self.completion_time - self.arrival_time
        
    # response time   = first run time  - arrival time (time until a job starts producing results)
    def compute_response_time(self) -> float:
        return self.first_run_time - self.arrival_time

    # wait time       = turnaround time - run time   (time spent in the ready queue waiting to run)
    def compute_wait_time(self) -> float:
        return self.compute_turnaround_time() - self.total_run_time
    
    def __repr__(self) -> str:
        return f"Job(job_number={self.job_number}, total_run_time={self.total_run_time}, arrival_time={self.arrival_time}, first_run_time={self.first_run_time}, completion_time={self.completion_time})"

    # required for priority queue
    def __lt__(self, other: Job):
        return self.get_arrival_time() < other.get_arrival_time()

class Scheduler:
    def __init__(self, quantum: int) -> None:
        self.queue = Queue()
        self.completed_jobs: list[Job] = []
        self.schedule = self.scheduler_fifo
        self.quantum: int = quantum
        self.total_time: int = 0
        self.print_str: str = ""
    
    def submit_jobs(self, jobs: list[Job]) -> None:
        for job in jobs:
            # priority for srjn is based on smallest run_time
            if self.schedule == self.scheduler_srtn:
                self.queue.put((job.get_run_time(), job))
            # assume everything has equal priority for rr (this may break)
            elif self.schedule == self.scheduler_rr:
                self.queue.put((1, job))
            # assume fifo priority is job number
            else:
                self.queue.put((job.get_job_number(), job))

    def submit_job(self, job: Job) -> None:
        if self.schedule == self.scheduler_srtn:
            self.queue.put((job.run_time, job))
        else:
            self.queue.put(job)

    def set_scheduling_func(self, schedule_func) -> None:
        self.schedule = schedule_func

    def start_scheduler(self) -> None:
        self.schedule()

    def reset(self) -> None:
        self.queue = PriorityQueue()
        self.completed_jobs = []
        self.total_time = 0
        self.print_str = ""

    def scheduler_rr(self) -> None:
        while not self.queue.empty():
            _, scheduled = self.queue.get()

            # if first time running, set total time to be first job arrival time
            if self.total_time == 0:
               self.total_time += scheduled.get_arrival_time()
               self.print_str += "[--]" * scheduled.get_arrival_time()

            if scheduled.run_time <= self.quantum:
                self.total_time += scheduled.run_time
                scheduled.set_completion_time(self.total_time)
                self.completed_jobs.append(scheduled)
                self.print_str += f"[P{scheduled.job_number}]" * scheduled.get_run_time()
                scheduled.set_run_time(0)
                continue
            if scheduled.is_first_time_running():
                scheduled.set_first_run_time(self.total_time)
                scheduled.set_is_first_time_running(False)
            scheduled.set_run_time(scheduled.get_run_time() - self.quantum)
            self.total_time += self.quantum
            self.print_str += f"[P{scheduled.job_number}]" * self.quantum 
            self.queue.put((1, scheduled))
 
    def scheduler_fifo(self) -> None:
        while not self.queue.empty():
            _, scheduled = self.queue.get()

            # if first time running, set total time to be first job arrival time
            if self.total_time == 0:
               self.total_time += scheduled.get_arrival_time()
               self.print_str += "[--]" * scheduled.get_arrival_time()

            scheduled.first_run_time = False
            scheduled.set_first_run_time(self.total_time)
            self.total_time += scheduled.get_run_time()
            self.print_str += f"[P{scheduled.job_number}]" * scheduled.get_run_time()
            scheduled.set_completion_time(self.total_time)
            scheduled.set_run_time(0)
            self.completed_jobs.append(scheduled)
    
    def scheduler_srtn(self) -> None:
        while not self.queue.empty():
            _, scheduled = self.queue.get()

    
    def stat(self) -> None:
        self.completed_jobs.sort(key=lambda job: job.job_number)
        for job in self.completed_jobs:
            print(job)
            # print(f"Job {job.job_number:3d} -- Turnaround {job.compute_turnaround_time():3.2f}  Wait {job.compute_wait_time():3.2f}")
        turnaround_average: float = sum([job.compute_turnaround_time() for job in self.completed_jobs]) / len(self.completed_jobs)
        waiting_average: float = sum([job.compute_wait_time() for job in self.completed_jobs]) / len(self.completed_jobs)
        print(f"Average -- Turnaround {turnaround_average:3.2f}  Wait {waiting_average:3.2f}")
        print(self.print_str)


def main() -> None:
    # argument setup
    parser: ArgumentParser = ArgumentParser()
    parser.add_argument("file", type=str, default="job_list.txt")
    # file: str = "./fcfs1.txt"
    parser.add_argument("-p", "--algorithm", required=False, type=str.lower, default="fifo", choices=["fifo", "rr", "srtn"])
    parser.add_argument("-q",  "--quantum", required=False, type=int, default=1)
    args: Namespace = parser.parse_args()
    scheduler: Scheduler = Scheduler(args.quantum)

    match(args.algorithm):
        case "rr":
            scheduler.set_scheduling_func(scheduler.scheduler_rr)
        case "srtn":
            scheduler.set_scheduling_func(scheduler.scheduler_srtn)
    
    scheduler.submit_jobs(read_jobs(args.file))
    scheduler.start_scheduler()
    scheduler.stat()

if __name__ == "__main__":
    main()